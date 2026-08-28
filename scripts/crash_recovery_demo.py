#!/usr/bin/env python3
"""Force-kill a claiming worker and verify lease-based recovery end to end."""

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


PROJECT_ROOT = Path(__file__).resolve().parents[1]
API_URL = os.getenv("RELAY_API_URL", "http://localhost:8000")
LEASE_SECONDS = "5"
JOB_DURATION_MS = 2500


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


def wait_for_state(
    job_id: str,
    predicate: Any,
    description: str,
    timeout_seconds: float,
) -> dict[str, Any]:
    deadline = time.monotonic() + timeout_seconds
    last_job: dict[str, Any] | None = None
    while time.monotonic() < deadline:
        last_job = request_json(f"/jobs/{job_id}")
        if predicate(last_job):
            print(
                f"observed={description} status={last_job['status']} "
                f"attempts={last_job['attempts']} "
                f"lease_expires_at={last_job['lease_expires_at']}"
            )
            return last_job
        time.sleep(0.05)
    raise RuntimeError(
        f"job did not reach {description} within {timeout_seconds}s; "
        f"last state: {last_job}"
    )


def worker_container_ids() -> list[str]:
    output = compose("ps", "-q", "worker", capture_output=True)
    return [line for line in output.splitlines() if line]


def claiming_container(job_id: str, timeout_seconds: float = 3) -> str:
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        for container_id in worker_container_ids():
            logs = subprocess.run(
                ["docker", "logs", container_id],
                check=True,
                text=True,
                capture_output=True,
            )
            combined = logs.stdout + logs.stderr
            if f"event=claimed job={job_id}" in combined:
                return container_id
        time.sleep(0.05)
    raise RuntimeError(f"could not identify the worker that claimed job {job_id}")


def main() -> int:
    sys.stdout.reconfigure(line_buffering=True)
    try:
        print("starting PostgreSQL, API, and two workers with a 5-second lease")
        compose("up", "-d", "--build", "--scale", "worker=2")

        created = request_json(
            "/jobs",
            {"payload": {"type": "sleep", "duration_ms": JOB_DURATION_MS}},
        )
        job_id = created["id"]
        print(f"submitted job={job_id} duration_ms={JOB_DURATION_MS}")

        wait_for_state(
            job_id,
            lambda job: job["status"] == "running" and job["attempts"] == 1,
            "initial claim",
            5,
        )
        victim = claiming_container(job_id)
        print(f"force_killing_container={victim}")
        subprocess.run(["docker", "kill", "--signal", "KILL", victim], check=True)

        time.sleep(0.5)
        stranded = request_json(f"/jobs/{job_id}")
        if stranded["status"] != "running" or stranded["attempts"] != 1:
            raise RuntimeError(f"job did not remain temporarily stranded: {stranded}")
        print("observed=temporarily_stranded status=running attempts=1")

        wait_for_state(
            job_id,
            lambda job: job["status"] == "running" and job["attempts"] == 2,
            "reclaim after lease expiry",
            8,
        )
        succeeded = wait_for_state(
            job_id,
            lambda job: job["status"] == "succeeded",
            "successful recovery",
            5,
        )
        if succeeded["attempts"] != 2 or succeeded["lease_expires_at"] is not None:
            raise RuntimeError(f"unexpected recovered state: {succeeded}")

        print("\nRelevant worker logs:")
        compose("logs", "--no-color", "worker")
        print("\nPASS: another worker reclaimed and completed the force-killed job")
        return 0
    except (RuntimeError, subprocess.CalledProcessError, urllib.error.URLError) as error:
        print(f"FAIL: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
