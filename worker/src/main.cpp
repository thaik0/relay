#include <libpq-fe.h>

#include <charconv>
#include <chrono>
#include <csignal>
#include <cstdlib>
#include <iostream>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>

namespace {

constexpr auto poll_interval = std::chrono::milliseconds(250);
constexpr auto reconnect_interval = std::chrono::seconds(1);
constexpr long long max_sleep_duration_ms = 60'000;

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
};

std::string postgres_error(PGconn* connection) {
    std::string message = PQerrorMessage(connection);
    while (!message.empty() && (message.back() == '\n' || message.back() == '\r')) {
        message.pop_back();
    }
    return message;
}

Result execute(PGconn* connection, const char* sql, ExecStatusType expected) {
    Result result(PQexec(connection, sql));
    if (!result || PQresultStatus(result.get()) != expected) {
        throw std::runtime_error(postgres_error(connection));
    }
    return result;
}

Result execute_with_id(
    PGconn* connection,
    const char* sql,
    const std::string& id,
    ExecStatusType expected
) {
    const char* values[] = {id.c_str()};
    Result result(PQexecParams(connection, sql, 1, nullptr, values, nullptr, nullptr, 0));
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

std::optional<Job> claim_job(PGconn* connection) {
    Transaction transaction(connection);

    Result selected = execute(
        connection,
        R"SQL(
            SELECT
                id::text,
                payload->>'type',
                jsonb_typeof(payload->'type'),
                payload->>'duration_ms',
                jsonb_typeof(payload->'duration_ms')
            FROM jobs
            WHERE status = 'queued'
              AND available_at <= CURRENT_TIMESTAMP
            ORDER BY available_at, created_at, id
            LIMIT 1
            FOR UPDATE
        )SQL",
        PGRES_TUPLES_OK
    );

    if (PQntuples(selected.get()) == 0) {
        transaction.commit();
        return std::nullopt;
    }

    Job job{
        .id = PQgetvalue(selected.get(), 0, 0),
        .type = optional_value(selected.get(), 0, 1),
        .type_json_kind = optional_value(selected.get(), 0, 2),
        .duration_ms = optional_value(selected.get(), 0, 3),
        .duration_json_kind = optional_value(selected.get(), 0, 4),
    };

    Result updated = execute_with_id(
        connection,
        R"SQL(
            UPDATE jobs
            SET status = 'running', attempts = attempts + 1
            WHERE id = $1::uuid
        )SQL",
        job.id,
        PGRES_COMMAND_OK
    );
    if (std::string_view(PQcmdTuples(updated.get())) != "1") {
        throw std::runtime_error("claim update did not affect exactly one job");
    }

    transaction.commit();
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

void execute_job(const Job& job) {
    if (job.type_json_kind != "string" || !job.type) {
        throw std::runtime_error("job type must be a string");
    }
    if (*job.type != "sleep") {
        throw std::runtime_error("unsupported job type: " + *job.type);
    }

    const long long duration_ms = parse_duration_ms(job);
    std::cout << "executing sleep job " << job.id << " for " << duration_ms << " ms"
              << std::endl;
    std::this_thread::sleep_for(std::chrono::milliseconds(duration_ms));
}

void mark_terminal(PGconn* connection, const Job& job, std::string_view status) {
    const char* sql = status == "succeeded"
        ? R"SQL(
            UPDATE jobs
            SET status = 'succeeded', completed_at = CURRENT_TIMESTAMP
            WHERE id = $1::uuid AND status = 'running'
        )SQL"
        : R"SQL(
            UPDATE jobs
            SET status = 'failed', completed_at = CURRENT_TIMESTAMP
            WHERE id = $1::uuid AND status = 'running'
        )SQL";

    Result updated = execute_with_id(connection, sql, job.id, PGRES_COMMAND_OK);
    if (std::string_view(PQcmdTuples(updated.get())) != "1") {
        throw std::runtime_error("terminal update did not affect exactly one running job");
    }
}

void sleep_before_retry(std::chrono::milliseconds duration) {
    std::this_thread::sleep_for(duration);
}

}  // namespace

int main() {
    std::signal(SIGINT, handle_signal);
    std::signal(SIGTERM, handle_signal);

    const char* database_url = std::getenv("DATABASE_URL");
    if (database_url == nullptr || *database_url == '\0') {
        std::cerr << "DATABASE_URL environment variable is required" << std::endl;
        return EXIT_FAILURE;
    }

    std::cout << "worker started" << std::endl;
    Connection connection;

    while (!stop_requested) {
        try {
            if (!connection || PQstatus(connection.get()) != CONNECTION_OK) {
                connection = connect_to_postgres(database_url);
                std::cout << "connected to PostgreSQL" << std::endl;
            }

            std::optional<Job> job = claim_job(connection.get());
            if (!job) {
                sleep_before_retry(poll_interval);
                continue;
            }

            std::cout << "claimed job " << job->id << std::endl;
            try {
                execute_job(*job);
            } catch (const std::exception& error) {
                std::cerr << "job " << job->id << " failed: " << error.what() << std::endl;
                mark_terminal(connection.get(), *job, "failed");
                continue;
            }

            mark_terminal(connection.get(), *job, "succeeded");
            std::cout << "job " << job->id << " succeeded" << std::endl;
        } catch (const std::exception& error) {
            std::cerr << "worker database error: " << error.what() << std::endl;
            connection.reset();
            if (!stop_requested) {
                sleep_before_retry(reconnect_interval);
            }
        }
    }

    std::cout << "worker stopped" << std::endl;
    return EXIT_SUCCESS;
}
