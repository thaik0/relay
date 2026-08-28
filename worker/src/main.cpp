#include <libpq-fe.h>

#include <charconv>
#include <chrono>
#include <cmath>
#include <csignal>
#include <cstdlib>
#include <iostream>
#include <limits>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

namespace {

constexpr auto poll_interval = std::chrono::milliseconds(250);
constexpr auto reconnect_interval = std::chrono::seconds(1);
constexpr long long max_sleep_duration_ms = 60'000;
constexpr std::string_view default_lease_seconds = "10";
constexpr std::string_view default_max_attempts = "3";
constexpr std::string_view default_retry_base_seconds = "1";

volatile std::sig_atomic_t stop_requested = 0;

void handle_signal(int) {
    stop_requested = 1;
}

struct ConnectionDeleter {
    void operator()(PGconn* connection) const {
        PQfinish(connection);
    }
};

struct ResultDeleter {
    void operator()(PGresult* result) const {
        PQclear(result);
    }
};

using Connection = std::unique_ptr<PGconn, ConnectionDeleter>;
using Result = std::unique_ptr<PGresult, ResultDeleter>;

struct Job {
    std::string id;
    std::optional<std::string> type;
    std::optional<std::string> type_json_kind;
    std::optional<std::string> duration_ms;
    std::optional<std::string> duration_json_kind;
    int attempt;
    std::string lease_expires_at;
    bool reclaimed;
};

struct WorkerConfig {
    std::string database_url;
    std::string worker;
    std::string lease_seconds;
    int max_attempts;
    std::string max_attempts_sql;
    std::string retry_base_seconds;
};

struct FailureTransition {
    bool retry_scheduled;
    std::string available_at;
};

std::string postgres_error(PGconn* connection) {
    std::string message = PQerrorMessage(connection);
    while (!message.empty() && (message.back() == '\n' || message.back() == '\r')) {
        message.pop_back();
    }
    return message;
}

std::string worker_id() {
    const char* configured = std::getenv("WORKER_ID");
    if (configured != nullptr && *configured != '\0') {
        return configured;
    }

    const char* hostname = std::getenv("HOSTNAME");
    if (hostname != nullptr && *hostname != '\0') {
        return std::string("worker-") + hostname;
    }

    return "worker-unknown";
}

Result execute(PGconn* connection, const char* sql, ExecStatusType expected) {
    Result result(PQexec(connection, sql));
    if (!result || PQresultStatus(result.get()) != expected) {
        throw std::runtime_error(postgres_error(connection));
    }
    return result;
}

Result execute_with_params(
    PGconn* connection,
    const char* sql,
    const std::vector<std::string>& parameters,
    ExecStatusType expected
) {
    std::vector<const char*> values;
    values.reserve(parameters.size());
    for (const std::string& parameter : parameters) {
        values.push_back(parameter.c_str());
    }

    Result result(PQexecParams(
        connection,
        sql,
        static_cast<int>(values.size()),
        nullptr,
        values.data(),
        nullptr,
        nullptr,
        0
    ));
    if (!result || PQresultStatus(result.get()) != expected) {
        throw std::runtime_error(postgres_error(connection));
    }
    return result;
}

std::optional<std::string> optional_value(PGresult* result, int row, int column) {
    if (PQgetisnull(result, row, column)) {
        return std::nullopt;
    }
    return std::string(PQgetvalue(result, row, column));
}

class Transaction {
public:
    explicit Transaction(PGconn* connection) : connection_(connection) {
        execute(connection_, "BEGIN", PGRES_COMMAND_OK);
    }

    Transaction(const Transaction&) = delete;
    Transaction& operator=(const Transaction&) = delete;

    ~Transaction() {
        if (!finished_) {
            Result ignored(PQexec(connection_, "ROLLBACK"));
        }
    }

    void commit() {
        execute(connection_, "COMMIT", PGRES_COMMAND_OK);
        finished_ = true;
    }

private:
    PGconn* connection_;
    bool finished_ = false;
};

Connection connect_to_postgres(const std::string& database_url) {
    Connection connection(PQconnectdb(database_url.c_str()));
    if (!connection || PQstatus(connection.get()) != CONNECTION_OK) {
        const std::string message = connection ? postgres_error(connection.get())
                                               : "libpq could not allocate a connection";
        throw std::runtime_error(message);
    }
    return connection;
}

std::string positive_number_setting(const char* name, std::string_view fallback) {
    const char* configured = std::getenv(name);
    const std::string value = configured != nullptr && *configured != '\0'
        ? configured
        : std::string(fallback);

    std::size_t parsed_length = 0;
    double parsed = 0;
    try {
        parsed = std::stod(value, &parsed_length);
    } catch (const std::exception&) {
        throw std::runtime_error(std::string(name) + " must be a positive number");
    }
    if (parsed_length != value.size() || !std::isfinite(parsed) || parsed <= 0) {
        throw std::runtime_error(std::string(name) + " must be a positive number");
    }
    return value;
}

int positive_integer_setting(const char* name, std::string_view fallback) {
    const char* configured = std::getenv(name);
    const std::string value = configured != nullptr && *configured != '\0'
        ? configured
        : std::string(fallback);

    long long parsed = 0;
    const char* begin = value.data();
    const char* end = begin + value.size();
    const auto [position, error] = std::from_chars(begin, end, parsed);
    if (
        error != std::errc{} || position != end || parsed <= 0
        || parsed > std::numeric_limits<int>::max()
    ) {
        throw std::runtime_error(std::string(name) + " must be a positive integer");
    }
    return static_cast<int>(parsed);
}

WorkerConfig load_config() {
    const char* database_url = std::getenv("DATABASE_URL");
    if (database_url == nullptr || *database_url == '\0') {
        throw std::runtime_error("DATABASE_URL environment variable is required");
    }

    const int max_attempts = positive_integer_setting(
        "MAX_JOB_ATTEMPTS",
        default_max_attempts
    );
    return WorkerConfig{
        .database_url = database_url,
        .worker = worker_id(),
        .lease_seconds = positive_number_setting(
            "JOB_LEASE_SECONDS",
            default_lease_seconds
        ),
        .max_attempts = max_attempts,
        .max_attempts_sql = std::to_string(max_attempts),
        .retry_base_seconds = positive_number_setting(
            "RETRY_BASE_SECONDS",
            default_retry_base_seconds
        ),
    };
}

std::optional<Job> claim_job(PGconn* connection, const WorkerConfig& config) {
    Transaction transaction(connection);

    Result exhausted = execute_with_params(
        connection,
        R"SQL(
            WITH candidate AS (
                SELECT id
                FROM jobs
                WHERE status = 'running'
                  AND lease_expires_at <= CURRENT_TIMESTAMP
                  AND attempts >= $1::integer
                ORDER BY lease_expires_at, created_at, id
                LIMIT 1
                FOR UPDATE SKIP LOCKED
            )
            UPDATE jobs AS job
            SET
                status = 'failed',
                completed_at = CURRENT_TIMESTAMP,
                lease_expires_at = NULL
            FROM candidate
            WHERE job.id = candidate.id
            RETURNING job.id::text, job.attempts::text
        )SQL",
        {config.max_attempts_sql},
        PGRES_TUPLES_OK
    );

    Result claimed = execute_with_params(
        connection,
        R"SQL(
            WITH candidate AS (
                SELECT id, status AS previous_status
                FROM jobs
                WHERE attempts < $2::integer
                  AND (
                      (status = 'queued' AND available_at <= CURRENT_TIMESTAMP)
                      OR (
                          status = 'running'
                          AND lease_expires_at <= CURRENT_TIMESTAMP
                      )
                  )
                ORDER BY
                    CASE WHEN status = 'running' THEN 0 ELSE 1 END,
                    COALESCE(lease_expires_at, available_at),
                    created_at,
                    id
                LIMIT 1
                -- Selection and transition share this transaction. Competing
                -- workers skip this candidate rather than claiming it twice.
                FOR UPDATE SKIP LOCKED
            )
            UPDATE jobs AS job
            SET
                status = 'running',
                attempts = job.attempts + 1,
                lease_expires_at = CURRENT_TIMESTAMP
                    + ($1::double precision * INTERVAL '1 second'),
                completed_at = NULL
            FROM candidate
            WHERE job.id = candidate.id
            RETURNING
                job.id::text,
                job.payload->>'type',
                jsonb_typeof(job.payload->'type'),
                job.payload->>'duration_ms',
                jsonb_typeof(job.payload->'duration_ms'),
                job.attempts::text,
                job.lease_expires_at::text,
                candidate.previous_status
        )SQL",
        {config.lease_seconds, config.max_attempts_sql},
        PGRES_TUPLES_OK
    );

    std::optional<Job> job;
    if (PQntuples(claimed.get()) == 1) {
        job = Job{
            .id = PQgetvalue(claimed.get(), 0, 0),
            .type = optional_value(claimed.get(), 0, 1),
            .type_json_kind = optional_value(claimed.get(), 0, 2),
            .duration_ms = optional_value(claimed.get(), 0, 3),
            .duration_json_kind = optional_value(claimed.get(), 0, 4),
            .attempt = std::stoi(PQgetvalue(claimed.get(), 0, 5)),
            .lease_expires_at = PQgetvalue(claimed.get(), 0, 6),
            .reclaimed = std::string_view(PQgetvalue(claimed.get(), 0, 7)) == "running",
        };
    }
    transaction.commit();

    if (PQntuples(exhausted.get()) == 1) {
        std::cout << "worker=" << config.worker
                  << " event=failed_permanently job="
                  << PQgetvalue(exhausted.get(), 0, 0)
                  << " attempt=" << PQgetvalue(exhausted.get(), 0, 1)
                  << " reason=lease_expired" << std::endl;
    }
    return job;
}

long long parse_duration_ms(const Job& job) {
    if (job.duration_json_kind != "number" || !job.duration_ms) {
        throw std::runtime_error("duration_ms must be an integer number");
    }

    long long duration = 0;
    const char* begin = job.duration_ms->data();
    const char* end = begin + job.duration_ms->size();
    const auto [position, error] = std::from_chars(begin, end, duration);
    if (error != std::errc{} || position != end) {
        throw std::runtime_error("duration_ms must be an integer number");
    }
    if (duration < 0 || duration > max_sleep_duration_ms) {
        throw std::runtime_error("duration_ms must be between 0 and 60000");
    }
    return duration;
}

void execute_job(const Job& job, const std::string& worker) {
    if (job.type_json_kind != "string" || !job.type) {
        throw std::runtime_error("job type must be a string");
    }
    if (*job.type == "fail") {
        throw std::runtime_error("deterministic failure requested");
    }
    if (*job.type != "sleep") {
        throw std::runtime_error("unsupported job type: " + *job.type);
    }

    const long long duration_ms = parse_duration_ms(job);
    std::cout << "worker=" << worker << " event=executing job=" << job.id
              << " duration_ms=" << duration_ms << std::endl;
    std::this_thread::sleep_for(std::chrono::milliseconds(duration_ms));
}

bool record_success(PGconn* connection, const Job& job) {
    Result updated = execute_with_params(
        connection,
        R"SQL(
            UPDATE jobs
            SET
                status = 'succeeded',
                completed_at = CURRENT_TIMESTAMP,
                lease_expires_at = NULL
            WHERE id = $1::uuid
              AND status = 'running'
              AND attempts = $2::integer
        )SQL",
        {job.id, std::to_string(job.attempt)},
        PGRES_COMMAND_OK
    );
    return std::string_view(PQcmdTuples(updated.get())) == "1";
}

std::optional<FailureTransition> record_failure(
    PGconn* connection,
    const Job& job,
    const WorkerConfig& config
) {
    Result updated = execute_with_params(
        connection,
        R"SQL(
            UPDATE jobs
            SET
                status = CASE
                    WHEN attempts < $3::integer THEN 'queued'
                    ELSE 'failed'
                END,
                available_at = CASE
                    WHEN attempts < $3::integer THEN
                        CURRENT_TIMESTAMP
                        + (
                            $4::double precision
                            * power(2::double precision, attempts - 1)
                            * INTERVAL '1 second'
                        )
                    ELSE available_at
                END,
                completed_at = CASE
                    WHEN attempts < $3::integer THEN NULL
                    ELSE CURRENT_TIMESTAMP
                END,
                lease_expires_at = NULL
            WHERE id = $1::uuid
              AND status = 'running'
              AND attempts = $2::integer
            RETURNING status, available_at::text
        )SQL",
        {
            job.id,
            std::to_string(job.attempt),
            config.max_attempts_sql,
            config.retry_base_seconds,
        },
        PGRES_TUPLES_OK
    );
    if (PQntuples(updated.get()) == 0) {
        return std::nullopt;
    }
    return FailureTransition{
        .retry_scheduled = std::string_view(PQgetvalue(updated.get(), 0, 0)) == "queued",
        .available_at = PQgetvalue(updated.get(), 0, 1),
    };
}

void sleep_before_retry(std::chrono::milliseconds duration) {
    std::this_thread::sleep_for(duration);
}

}  // namespace

int main() {
    std::signal(SIGINT, handle_signal);
    std::signal(SIGTERM, handle_signal);

    WorkerConfig config;
    try {
        config = load_config();
    } catch (const std::exception& error) {
        std::cerr << error.what() << std::endl;
        return EXIT_FAILURE;
    }

    std::cout << "worker=" << config.worker << " event=started"
              << " lease_seconds=" << config.lease_seconds
              << " max_attempts=" << config.max_attempts
              << " retry_base_seconds=" << config.retry_base_seconds << std::endl;
    Connection connection;

    while (!stop_requested) {
        try {
            if (!connection || PQstatus(connection.get()) != CONNECTION_OK) {
                connection = connect_to_postgres(config.database_url);
                std::cout << "worker=" << config.worker << " event=connected"
                          << std::endl;
            }

            std::optional<Job> job = claim_job(connection.get(), config);
            if (!job) {
                sleep_before_retry(poll_interval);
                continue;
            }

            std::cout << "worker=" << config.worker
                      << " event=" << (job->reclaimed ? "reclaimed" : "claimed")
                      << " job=" << job->id << " attempt=" << job->attempt
                      << " lease_expires_at=\"" << job->lease_expires_at << "\""
                      << std::endl;
            try {
                execute_job(*job, config.worker);
            } catch (const std::exception& error) {
                std::cerr << "worker=" << config.worker << " event=failed job="
                          << job->id << " attempt=" << job->attempt
                          << " error=\"" << error.what() << "\"" << std::endl;
                const std::optional<FailureTransition> transition = record_failure(
                    connection.get(),
                    *job,
                    config
                );
                if (!transition) {
                    std::cout << "worker=" << config.worker
                              << " event=completion_superseded job=" << job->id
                              << " attempt=" << job->attempt << std::endl;
                } else if (transition->retry_scheduled) {
                    std::cout << "worker=" << config.worker
                              << " event=retry_scheduled job=" << job->id
                              << " attempt=" << job->attempt
                              << " available_at=\"" << transition->available_at
                              << "\"" << std::endl;
                } else {
                    std::cout << "worker=" << config.worker
                              << " event=failed_permanently job=" << job->id
                              << " attempt=" << job->attempt
                              << " reason=execution_failed" << std::endl;
                }
                continue;
            }

            if (record_success(connection.get(), *job)) {
                std::cout << "worker=" << config.worker << " event=succeeded job="
                          << job->id << " attempt=" << job->attempt << std::endl;
            } else {
                std::cout << "worker=" << config.worker
                          << " event=completion_superseded job=" << job->id
                          << " attempt=" << job->attempt << std::endl;
            }
        } catch (const std::exception& error) {
            std::cerr << "worker=" << config.worker
                      << " event=database_error error=\"" << error.what() << "\""
                      << std::endl;
            connection.reset();
            if (!stop_requested) {
                sleep_before_retry(reconnect_interval);
            }
        }
    }

    std::cout << "worker=" << config.worker << " event=stopped" << std::endl;
    return EXIT_SUCCESS;
}
