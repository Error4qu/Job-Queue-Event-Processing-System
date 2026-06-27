"""
Maps self-healer healing_strategies.cpp actions to admin API execution.

The C++ self-healer is the source of truth for which action applies to each
incident type. This module only translates those actions into HTTP calls.
"""

from app.models import AnalysisResult, IncidentPayload, PlannedAction, SelfHealerOutput

# Mirrors healing_strategies.cpp action names and safe execution policy.
ACTION_POLICY: dict[str, dict] = {
    "RECOVER_MISSED_SCHEDULE_WINDOWS": {
        "endpoint": "/admin/watcher/recover",
        "auto_execute": True,
        "confidence": 0.95,
    },
    "RECONCILE_REDIS_DELAY_QUEUE": {
        "endpoint": "/admin/redis/reconcile",
        "auto_execute": True,
        "confidence": 0.9,
    },
    "WAIT_FOR_RETRY_WINDOW": {
        "endpoint": None,
        "auto_execute": False,
        "confidence": 0.85,
    },
    "PAUSE_AND_ESCALATE_JOB_TYPE": {
        "endpoint": None,
        "auto_execute": False,
        "confidence": 0.8,
    },
    "VERIFY_RETRY_OR_FINAL_STATE": {
        "endpoint": None,
        "auto_execute": False,
        "confidence": 0.75,
    },
    "ALERT_OPERATOR": {
        "endpoint": None,
        "auto_execute": False,
        "confidence": 0.5,
    },
}

SEVERITY_CONFIDENCE = {
    "CRITICAL": 0.9,
    "HIGH": 0.85,
    "MEDIUM": 0.8,
    "LOW": 0.7,
}


def analyze_from_self_healer(output: SelfHealerOutput, health: dict | None = None) -> AnalysisResult:
    incident = output.incident
    plan = output.plan
    policy = ACTION_POLICY.get(plan.action, ACTION_POLICY["ALERT_OPERATOR"])

    return AnalysisResult(
        root_cause=incident.title or incident.evidence,
        impact=_impact(incident.severity, health),
        recommended_action=plan.action,
        confidence=max(policy["confidence"], SEVERITY_CONFIDENCE.get(incident.severity.upper(), 0.7)),
        reasoning=_reasoning(plan),
        used_llm=False,
        strategy=plan.strategy,
    )


def plan_from_self_healer(output: SelfHealerOutput, analysis: AnalysisResult) -> PlannedAction:
    plan = output.plan
    incident = output.incident
    policy = ACTION_POLICY.get(plan.action, ACTION_POLICY["ALERT_OPERATOR"])

    params: dict = {}
    endpoint = policy["endpoint"]
    auto_execute = policy["auto_execute"]

    if plan.action == "VERIFY_RETRY_OR_FINAL_STATE" and incident.job_id:
        params = {"job_id": incident.job_id, "verify_only": True}

    return PlannedAction(
        action=plan.action,
        endpoint=endpoint,
        params=params,
        auto_execute=auto_execute,
        confidence=analysis.confidence,
        rationale=_reasoning(plan),
        strategy=plan.strategy,
        steps=plan.steps,
        verification_checks=plan.verification_checks,
    )


def analyze_with_rules(incident: IncidentPayload, health: dict | None = None) -> AnalysisResult:
    """Fallback when self-healer binary is unavailable."""
    if incident.self_healer:
        return analyze_from_self_healer(incident.self_healer, health)

    action = _fallback_action(incident.type)
    policy = ACTION_POLICY.get(action, ACTION_POLICY["ALERT_OPERATOR"])

    return AnalysisResult(
        root_cause=incident.title or incident.evidence or incident.type,
        impact=_impact(incident.severity, health),
        recommended_action=action,
        confidence=policy["confidence"],
        reasoning=f"Fallback rule for {incident.type}",
        used_llm=False,
    )


def plan_with_rules(analysis: AnalysisResult, incident: IncidentPayload) -> PlannedAction:
    if incident.self_healer:
        return plan_from_self_healer(incident.self_healer, analysis)

    policy = ACTION_POLICY.get(analysis.recommended_action, ACTION_POLICY["ALERT_OPERATOR"])
    params: dict = {}
    if analysis.recommended_action == "VERIFY_RETRY_OR_FINAL_STATE" and incident.job_id:
        params = {"job_id": incident.job_id, "verify_only": True}

    return PlannedAction(
        action=analysis.recommended_action,
        endpoint=policy["endpoint"],
        params=params,
        auto_execute=policy["auto_execute"],
        confidence=analysis.confidence,
        rationale=analysis.reasoning,
        steps=[],
        verification_checks=[],
    )


def _fallback_action(incident_type: str) -> str:
    mapping = {
        "WATCHER_GAP": "RECOVER_MISSED_SCHEDULE_WINDOWS",
        "REDIS_DISPATCH_ANOMALY": "RECONCILE_REDIS_DELAY_QUEUE",
        "RATE_LIMITED_JOB": "WAIT_FOR_RETRY_WINDOW",
        "CIRCUIT_OPEN": "PAUSE_AND_ESCALATE_JOB_TYPE",
        "JOB_EXECUTION_FAILED": "VERIFY_RETRY_OR_FINAL_STATE",
    }
    return mapping.get(incident_type.upper(), "ALERT_OPERATOR")


def _reasoning(plan) -> str:
    parts = [f"Strategy: {plan.strategy}", f"Action: {plan.action}"]
    if plan.steps:
        parts.append(f"Steps: {'; '.join(plan.steps[:2])}")
    return " | ".join(parts)


def _impact(severity: str, health: dict | None) -> str:
    if severity.upper() in ("CRITICAL", "HIGH"):
        return "High impact on job processing reliability"
    if health and health.get("overdueScheduled", 0) > 0:
        return f"{health['overdueScheduled']} overdue scheduled jobs in system"
    return "Operational incident detected"
