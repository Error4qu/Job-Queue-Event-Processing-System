# Self-Healing Orchestrator

This service sits on top of the job queue and closes the loop between **incident detection** and **automated recovery**. It uses the C++ `self-healer` binary I built for log analysis and healing plans, then executes safe remediation through Spring Boot admin APIs.

## How it works

```
App logs / Kafka incidents
        │
        ▼
  C++ self-healer  →  { incident, plan }
        │
        ▼
  Orchestrator: observe → analyze → plan → execute
        │
        ▼
  Spring Boot /admin/* recovery APIs
```

| Component | What it does |
|-----------|--------------|
| `self-healer/` (C++) | Detects incidents from logs, picks a healing strategy |
| `ai-agent/` (Python) | Runs the orchestration pipeline, calls admin APIs |
| `log-bridge/` | Optional — pipes log files through self-healer into the orchestrator |

## Configuration (change these before production)

Set these in your `.env` or `docker-compose.yml`:

| Variable | Default | What to set |
|----------|---------|-------------|
| `ADMIN_API_KEY` | `change-me-in-production` | **Required.** Same value in both `app` and `ai-agent` services |
| `OPENAI_API_KEY` | (empty) | Optional — enables LLM-based root-cause analysis |
| `AUTO_EXECUTE` | `true` | Set `false` if you only want plans without auto-remediation |
| `MIN_CONFIDENCE` | `0.75` | Minimum confidence before auto-executing a recovery action |
| `DRY_RUN` | `false` | Set `true` to log actions without calling admin APIs |
| `JOB_QUEUE_BASE_URL` | `http://app:8080` | Point to your Spring Boot instance |

## API

| Endpoint | Description |
|----------|-------------|
| `GET /health` | Service status + self-healer availability |
| `POST /incidents` | Submit an incident (enriched via self-healer when possible) |
| `POST /incidents/self-healer` | Submit raw self-healer JSON output |
| `POST /incidents/analyze-logs` | Pipe log lines through C++ self-healer |
| `POST /incidents/demo-sample` | Run bundled `self-healer/examples/sample.txt` |
| `GET /runs` | View recent orchestration runs |

## Quick start

```bash
# From project root
export ADMIN_API_KEY=your-secure-key

docker compose --profile ai up -d --build
```

Test it:

```bash
curl -X POST http://localhost:8090/incidents/demo-sample
curl http://localhost:8090/health
```

## Healing actions

These come directly from `self-healer/src/healing_strategies.cpp`:

| Action | Orchestrator behavior |
|--------|----------------------|
| `RECOVER_MISSED_SCHEDULE_WINDOWS` | Calls `POST /admin/watcher/recover` |
| `RECONCILE_REDIS_DELAY_QUEUE` | Calls `POST /admin/redis/reconcile` |
| `WAIT_FOR_RETRY_WINDOW` | Monitors only — no mutation |
| `PAUSE_AND_ESCALATE_JOB_TYPE` | Escalates — does not force circuit reset |
| `VERIFY_RETRY_OR_FINAL_STATE` | Verifies job status via `GET /jobs/{id}` |
| `ALERT_OPERATOR` | Escalates for manual review |
