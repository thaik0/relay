#!/usr/bin/env python3
"""Crash after a side effect and show safe at-least-once re-execution."""

from __future__ import annotations

import json
import os
import subprocess
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any
from uuid import uuid4


PROJECT_ROOT = Path(__file__).resolve().parents[1]
API_URL = os.getenv("RELAY_API_URL", "http://localhost:8000")
LEASE_SECONDS = "2"


def compose(*arguments: str, capture_output: bool = False) -> str:
    environment = os.environ.copy()
    environment.update(
        {
            "JOB_LEASE_SECONDS": LEASE_SECONDS,
            "MAX_JOB_ATTEMPTS": "3",
            "RETRY_BASE_SECONDS": "0.25",
        }
    )
    result = subprocess.run(
        ["docker", "compose", *arguments],
        cwd=PROJECT_ROOT,
        env=environment,
        check=True,
        text=True,
        capture_output=capture_output,
    )
    return result.stdout.strip() if capture_output else ""


def request_json(path: str, payload: dict[str, Any] | None = None) -> dict[str, Any]:
    body = None if payload is None else json.dumps(payload).encode()
    request = urllib.request.Request(
        f"{API_URL}{path}",
        data=body,
        headers={"Content-Type": "application/json"},
        method="POST" if body is not None else "GET",
    )
    with urllib.request.urlopen(request, timeout=3) as response:
        return json.load(response)


def database_scalar(sql: str) -> str:
    return compose(
        "exec",
        "-T",
        "db",
        "psql",
        "-U",
        "relay",
        "-d",
        "relay",
        "-At",
        "-c",
        sql,
        capture_output=True,
    )


def wait_for(
    observation: Any,
    predicate: Any,
    description: str,
    timeout_seconds: float,
) -> Any:
    deadline = time.monotonic() + timeout_seconds
    last_value: Any = None
    while time.monotonic() < deadline:
        last_value = observation()
        if predicate(last_value):
            print(f"observed={description} value={last_value}")
            return last_value
        time.sleep(0.05)
    raise RuntimeError(
        f"did not observe {description} within {timeout_seconds}s; "
        f"last value: {last_value}"
    )


def main() -> int:
    sys.stdout.reconfigure(line_buffering=True)
    operation_id = f"operation-{uuid4()}"
    try:
        print("starting PostgreSQL, API, and two workers with a 2-second lease")
        compose("up", "-d", "--build", "--scale", "worker=2")

        created = request_json(
            "/jobs",
            {
                "payload": {
                    "type": "write_effect",
                    "operation_id": operation_id,
                    "value": "hello",
                    "crash_after_effect_on_attempt": 1,
                }
            },
        )
        job_id = created["id"]
        print(f"submitted job={job_id} operation_id={operation_id}")

        wait_for(
            lambda: int(
                database_scalar(
                    "SELECT count(*) FROM effect_attempts "
                    f"WHERE job_id = '{job_id}'::uuid"
                )
            ),
            lambda count: count >= 1,
            "first effect execution committed before acknowledgement",
            4,
        )
        stranded = request_json(f"/jobs/{job_id}")
        if stranded["status"] != "running" or stranded["attempts"] != 1:
            raise RuntimeError(f"job was not stranded after worker death: {stranded}")
        print("observed=post_effect_worker_death status=running attempts=1")

        completed = wait_for(
            lambda: request_json(f"/jobs/{job_id}"),
            lambda job: job["status"] == "succeeded",
            "reclaimed job succeeded",
            7,
        )
        if completed["attempts"] != 2:
            raise RuntimeError(f"expected exactly two attempts: {completed}")

        execution_count = int(
            database_scalar(
                "SELECT count(*) FROM effect_attempts "
                f"WHERE job_id = '{job_id}'::uuid"
            )
        )
        effect_count = int(
            database_scalar(
                "SELECT count(*) FROM effects "
                f"WHERE operation_id = '{operation_id}'"
            )
        )
        if execution_count != 2 or effect_count != 1:
            raise RuntimeError(
                f"expected executions=2 and effects=1; got "
                f"executions={execution_count}, effects={effect_count}"
            )

        print("\nRelevant worker logs:")
        logs = compose("logs", "--no-color", "worker", capture_output=True)
        for line in logs.splitlines():
            if job_id in line:
                print(line)
        print(
            "\nPASS: handler executions=2, logical effects=1, "
            "final status=succeeded"
        )
        return 0
    except (RuntimeError, subprocess.CalledProcessError, urllib.error.URLError) as error:
        print(f"FAIL: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
