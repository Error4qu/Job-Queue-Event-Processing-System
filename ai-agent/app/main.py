import structlog
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from app.agents.observer import ObserverAgent
from app.clients.self_healer import SelfHealerClient, resolve_sample_log_path
from app.config import settings
from app.models import AgentRunResult, IncidentPayload, SelfHealerOutput
from app.orchestrator import AgentOrchestrator

structlog.configure(
    processors=[
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.add_log_level,
        structlog.processors.JSONRenderer(),
    ]
)
log = structlog.get_logger()

orchestrator = AgentOrchestrator()
observer = ObserverAgent(on_incident=lambda inc: orchestrator.process_incident(inc))
self_healer_client = SelfHealerClient()


class LogLinesRequest(BaseModel):
    lines: list[str]


@asynccontextmanager
async def lifespan(_: FastAPI):
    observer.start()
    log.info(
        "ai_agent.started",
        port=settings.port,
        self_healer_available=self_healer_client.is_available(),
    )
    yield
    observer.stop()


app = FastAPI(
    title="Job Queue AI Agent",
    description="Self-healing orchestrator — uses the C++ self-healer for detection and planning",
    version="1.1.0",
    lifespan=lifespan,
)


@app.get("/health")
def health():
    return {
        "status": "UP",
        "service": settings.app_name,
        "auto_execute": settings.auto_execute,
        "dry_run": settings.dry_run,
        "llm_enabled": bool(settings.openai_api_key),
        "self_healer_available": self_healer_client.is_available(),
        "self_healer_bin": settings.self_healer_bin,
    }


@app.post("/incidents", response_model=AgentRunResult)
def ingest_incident(incident: IncidentPayload):
    """Accept incidents; evidence log lines are enriched via self-healer when possible."""
    return orchestrator.process_incident(incident)


@app.post("/incidents/self-healer", response_model=AgentRunResult)
def ingest_self_healer_payload(payload: dict):
    """Accept raw self-healer JSON (incident + plan) from log-bridge."""
    output = SelfHealerOutput.model_validate(payload)
    return orchestrator.process_self_healer_output(output)


@app.post("/incidents/analyze-logs", response_model=list[AgentRunResult])
def analyze_logs(request: LogLinesRequest):
    """Pipe raw application log lines through the C++ self-healer, then run agents."""
    if not self_healer_client.is_available():
        raise HTTPException(status_code=503, detail="self-healer binary not available")
    return orchestrator.process_log_lines(request.lines)


@app.post("/incidents/demo-sample", response_model=list[AgentRunResult])
def demo_sample_logs():
    """Run the bundled self-healer/examples/sample.txt through the full pipeline."""
    sample_path = resolve_sample_log_path()
    if not sample_path:
        raise HTTPException(status_code=404, detail="sample.txt not found")
    outputs = self_healer_client.analyze_log_file(sample_path)
    return [orchestrator.process_self_healer_output(o) for o in outputs]


@app.get("/self-healer/status")
def self_healer_status():
    return {
        "available": self_healer_client.is_available(),
        "binary": settings.self_healer_bin,
        "sample_log": resolve_sample_log_path(),
    }


@app.get("/runs", response_model=list[AgentRunResult])
def list_runs(limit: int = 20):
    return orchestrator.get_history(limit)


@app.get("/runs/{incident_id}", response_model=AgentRunResult)
def get_run(incident_id: str):
    for run in orchestrator.get_history(100):
        if run.incident_id == incident_id:
            return run
    raise HTTPException(status_code=404, detail="Run not found")
