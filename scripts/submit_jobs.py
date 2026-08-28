#!/usr/bin/env python3
"""Submit a small batch of sleep jobs and wait for their terminal states."""

import argparse
import json
import time
import urllib.error
import urllib.request
from collections import Counter
from typing import Any


TERMINAL_STATUSES = {"succeeded", "failed"}


def request_json(url: str, body: dict[str, Any] | None = None) -> dict[str, Any]:
    data = json.dumps(body).encode() if body is not None else None
    request = urllib.request.Request(
        url,
        data=data,
        headers={"Content-Type": "application/json"} if data else {},
        method="POST" if data else "GET",
    )
    try:
        with urllib.request.urlopen(request, timeout=5) as response:
            return json.load(response)
    except urllib.error.HTTPError as error:
        detail = error.read().decode(errors="replace")
        raise RuntimeError(f"request failed with HTTP {error.code}: {detail}") from error


def positive_int(value: str) -> int:
    parsed = int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("must be greater than zero")
    return parsed


def duration_ms(value: str) -> int:
    parsed = int(value)
    if not 0 <= parsed <= 60_000:
        raise argparse.ArgumentTypeError("must be between 0 and 60000")
    return parsed


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--count", type=positive_int, default=100)
    parser.add_argument("--duration-ms", type=duration_ms, default=100)
    parser.add_argument("--api-url", default="http://localhost:8000")
    parser.add_argument("--timeout", type=positive_int, default=30)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    jobs_url = f"{args.api_url.rstrip('/')}/jobs"
    job_ids = []
    for _ in range(args.count):
        created = request_json(
            jobs_url,
            {"payload": {"type": "sleep", "duration_ms": args.duration_ms}},
        )
        job_ids.append(created["id"])

    print(f"submitted={len(job_ids)} duration_ms={args.duration_ms}", flush=True)

    deadline = time.monotonic() + args.timeout
    latest: dict[str, str] = {}
    while time.monotonic() < deadline:
        latest = {
            job_id: request_json(f"{jobs_url}/{job_id}")["status"]
            for job_id in job_ids
        }
        counts = Counter(latest.values())
        print(
            " ".join(f"{status}={count}" for status, count in sorted(counts.items())),
            flush=True,
        )
        if all(status in TERMINAL_STATUSES for status in latest.values()):
            return 0 if counts.get("succeeded", 0) == len(job_ids) else 1
        time.sleep(0.25)

    counts = Counter(latest.values())
    print(f"timed out after {args.timeout}s: {dict(counts)}", flush=True)
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
