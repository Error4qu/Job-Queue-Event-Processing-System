#!/usr/bin/env python3
"""Forwards self-healer JSON output to the healing orchestrator service."""

import json
import os
import subprocess
import sys

import httpx
import structlog

structlog.configure(
    processors=[
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.add_log_level,
        structlog.processors.JSONRenderer(),
    ]
)
log = structlog.get_logger()

AI_AGENT_URL = os.getenv("AI_AGENT_URL", "http://ai-agent:8090")
SELF_HEALER_BIN = os.getenv("SELF_HEALER_BIN", "/usr/local/bin/self-healer")


def forward_to_agent(payload: dict) -> None:
    with httpx.Client(timeout=30.0) as client:
        response = client.post(f"{AI_AGENT_URL}/incidents/self-healer", json=payload)
        response.raise_for_status()
        result = response.json()
        log.info(
            "bridge.forwarded",
            type=payload.get("incident", {}).get("type"),
            strategy=result.get("self_healer_strategy"),
            action=result.get("plan", {}).get("action"),
        )


def process_log_file(path: str) -> None:
    proc = subprocess.run(
        [SELF_HEALER_BIN, "--log-file", path],
        capture_output=True,
        text=True,
        timeout=30,
    )
    for line in proc.stdout.splitlines():
        line = line.strip()
        if not line.startswith("{"):
            continue
        try:
            forward_to_agent(json.loads(line))
        except (json.JSONDecodeError, httpx.HTTPError) as exc:
            log.error("bridge.line_failed", error=str(exc))


def main() -> None:
    path = sys.argv[1] if len(sys.argv) > 1 else "/logs/sample.txt"
    log.info("bridge.starting", ai_agent=AI_AGENT_URL, log_file=path)
    process_log_file(path)
    log.info("bridge.completed")


if __name__ == "__main__":
    main()
