import structlog

from app.clients.self_healer import SelfHealerClient
from app.config import settings
from app.models import AnalysisResult, IncidentPayload
from app.rules.engine import analyze_from_self_healer, analyze_with_rules

log = structlog.get_logger()


class AnalyzerAgent:
    def __init__(self, client, self_healer: SelfHealerClient | None = None) -> None:
        self.client = client
        self.self_healer = self_healer or SelfHealerClient()

    def analyze(self, incident: IncidentPayload) -> AnalysisResult:
        health = None
        try:
            health = self.client.get_detailed_health()
        except Exception as exc:
            log.warning("analyzer.health_fetch_failed", error=str(exc))

        if incident.self_healer:
            return analyze_from_self_healer(incident.self_healer, health)

        if settings.openai_api_key:
            try:
                return self._analyze_with_llm(incident, health)
            except Exception as exc:
                log.warning("analyzer.llm_failed", error=str(exc))

        return analyze_with_rules(incident, health)

    def _analyze_with_llm(self, incident: IncidentPayload, health: dict | None) -> AnalysisResult:
        import json

        from openai import OpenAI

        client = OpenAI(api_key=settings.openai_api_key)
        context = {
            "incident": incident.model_dump(by_alias=True),
            "self_healer_plan": incident.plan,
            "system_health": health,
            "valid_actions": list(
                {
                    "RECOVER_MISSED_SCHEDULE_WINDOWS",
                    "RECONCILE_REDIS_DELAY_QUEUE",
                    "WAIT_FOR_RETRY_WINDOW",
                    "PAUSE_AND_ESCALATE_JOB_TYPE",
                    "VERIFY_RETRY_OR_FINAL_STATE",
                    "ALERT_OPERATOR",
                }
            ),
        }

        response = client.chat.completions.create(
            model=settings.openai_model,
            response_format={"type": "json_object"},
            messages=[
                {
                    "role": "system",
                    "content": (
                        "You are an SRE agent for a distributed job queue. "
                        "Prefer the self-healer plan action when provided. "
                        "Return JSON: root_cause, impact, recommended_action, confidence, reasoning."
                    ),
                },
                {"role": "user", "content": json.dumps(context)},
            ],
            temperature=0.1,
        )

        data = json.loads(response.choices[0].message.content or "{}")
        return AnalysisResult(
            root_cause=data.get("root_cause", "Unknown"),
            impact=data.get("impact", "Unknown"),
            recommended_action=data.get("recommended_action", "ALERT_OPERATOR"),
            confidence=float(data.get("confidence", 0.5)),
            reasoning=data.get("reasoning", "LLM analysis"),
            used_llm=True,
            strategy=incident.plan.get("strategy") if incident.plan else None,
        )
