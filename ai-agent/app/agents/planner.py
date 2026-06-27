from app.config import settings
from app.models import AnalysisResult, IncidentPayload, PlannedAction
from app.rules.engine import ACTION_POLICY, plan_from_self_healer, plan_with_rules


class PlannerAgent:
    def plan(self, incident: IncidentPayload, analysis: AnalysisResult) -> PlannedAction:
        if incident.self_healer:
            planned = plan_from_self_healer(incident.self_healer, analysis)
        else:
            planned = plan_with_rules(analysis, incident)

        if analysis.used_llm:
            planned = self._apply_llm_override(planned, analysis, incident)

        if not settings.auto_execute:
            planned.auto_execute = False

        if planned.confidence < settings.min_confidence:
            planned.auto_execute = False
            planned.rationale += (
                f" (confidence {planned.confidence:.2f} below threshold {settings.min_confidence})"
            )

        return planned

    def _apply_llm_override(
        self, planned: PlannedAction, analysis: AnalysisResult, incident: IncidentPayload
    ) -> PlannedAction:
        action = analysis.recommended_action.upper()
        policy = ACTION_POLICY.get(action, ACTION_POLICY["ALERT_OPERATOR"])

        planned.action = action
        planned.confidence = analysis.confidence
        planned.rationale = analysis.reasoning
        planned.endpoint = policy["endpoint"]
        planned.auto_execute = policy["auto_execute"]

        if action == "VERIFY_RETRY_OR_FINAL_STATE" and incident.job_id:
            planned.params = {"job_id": incident.job_id, "verify_only": True}

        return planned
