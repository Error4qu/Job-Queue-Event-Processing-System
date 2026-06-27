import threading
import uuid
from datetime import datetime, timezone

import structlog

from app.agents.analyzer import AnalyzerAgent
from app.agents.executor import ExecutorAgent
from app.agents.planner import PlannerAgent
from app.clients.job_queue import JobQueueClient
from app.clients.self_healer import SelfHealerClient
from app.models import AgentRunResult, IncidentPayload, SelfHealerOutput

log = structlog.get_logger()


class AgentOrchestrator:
    """Runs the self-healing pipeline: enrich → analyze → plan → execute."""

    def __init__(self) -> None:
        self.client = JobQueueClient()
        self.self_healer = SelfHealerClient()
        self.analyzer = AnalyzerAgent(self.client, self.self_healer)
        self.planner = PlannerAgent()
        self.executor = ExecutorAgent(self.client)
        self._lock = threading.Lock()
        self._history: list[AgentRunResult] = []
        self._max_history = 100

    def process_incident(self, incident: IncidentPayload) -> AgentRunResult:
        if not incident.incident_id:
            incident.incident_id = str(uuid.uuid4())
        if not incident.timestamp:
            incident.timestamp = datetime.now(timezone.utc)

        incident = self.self_healer.enrich_incident(incident)

        log.info(
            "orchestrator.processing",
            incident_id=incident.incident_id,
            type=incident.type,
            severity=incident.severity,
            self_healer=incident.self_healer is not None,
            strategy=incident.plan.get("strategy") if incident.plan else None,
        )

        analysis = self.analyzer.analyze(incident)
        plan = self.planner.plan(incident, analysis)
        execution = None
        escalated = False

        if plan.auto_execute and plan.endpoint:
            execution = self.executor.execute(plan)
            if not execution.success:
                escalated = True
        else:
            execution = self.executor.execute(plan)
            if plan.action in ("ALERT_OPERATOR", "PAUSE_AND_ESCALATE_JOB_TYPE"):
                escalated = True

        result = AgentRunResult(
            incident_id=incident.incident_id,
            incident_type=incident.type,
            analysis=analysis,
            plan=plan,
            execution=execution,
            escalated=escalated,
            self_healer_strategy=plan.strategy,
        )

        with self._lock:
            self._history.append(result)
            if len(self._history) > self._max_history:
                self._history = self._history[-self._max_history:]

        log.info(
            "orchestrator.completed",
            incident_id=incident.incident_id,
            action=plan.action,
            strategy=plan.strategy,
            executed=execution is not None and not execution.dry_run,
            escalated=escalated,
        )
        return result

    def process_self_healer_output(self, output: SelfHealerOutput) -> AgentRunResult:
        return self.process_incident(IncidentPayload.from_self_healer(output))

    def process_log_lines(self, lines: list[str]) -> list[AgentRunResult]:
        outputs = self.self_healer.analyze_log_lines(lines)
        return [self.process_self_healer_output(output) for output in outputs]

    def get_history(self, limit: int = 20) -> list[AgentRunResult]:
        with self._lock:
            return list(reversed(self._history[-limit:]))
