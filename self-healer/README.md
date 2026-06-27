# Self Healer

This is a separate C++ microservice for fast log filtering and incident planning.
It reads application logs, detects operational signals, creates a structured incident,
selects a healing strategy, and prints a dry-run repair plan as JSON.

## Current Scope

- Detect watcher gaps.
- Detect rate-limit pressure.
- Detect open circuit breaker events.
- Detect job execution failures.
- Detect Redis dispatch anomalies.
- Emit safe healing plans without mutating production state.

## Run Locally

```bash
cmake -S . -B build
cmake --build build
./build/self-healer --log-file ../logs/app.log --follow
```

If no `--log-file` is provided, the service reads from standard input.

```bash
docker compose logs app -f | ./build/self-healer
```

Try the included sample:

```bash
./build/self-healer --log-file examples/sample.txt
```

## Docker

```bash
docker compose --profile self-healer up --build self-healer
```

The Docker Compose service is intentionally behind a profile so it does not run
unless explicitly requested.
