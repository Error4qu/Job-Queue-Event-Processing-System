from datetime import datetime
from enum import Enum
from typing import Any

from pydantic import BaseModel, Field


class IncidentSeverity(str, Enum):
    LOW = "LOW"
    MEDIUM = "MEDIUM"
    HIGH = "HIGH"
    CRITICAL = "CRITICAL"


class SelfHealerIncident(BaseModel):
    """Matches self-healer json_writer incident block."""

    type: str
    severity: str = "MEDIUM"
    title: str = ""
    job_id: str = Field(default="", alias="jobId")
    job_type: str = Field(default="", alias="jobType")
    evidence: str = ""

    model_config = {"populate_by_name": True}


class SelfHealerPlan(BaseModel):
    """Matches self-healer json_writer plan block from healing_strategies.cpp."""

    strategy: str = ""
    action: str = ""
    dry_run: bool = Field(default=True, alias="dryRun")
    steps: list[str] = Field(default_factory=list)
    verification_checks: list[str] = Field(default_factory=list, alias="verificationChecks")

    model_config = {"populate_by_name": True}


class SelfHealerOutput(BaseModel):
    """Full JSON document emitted by the C++ self-healer binary."""

    incident: SelfHealerIncident
    plan: SelfHealerPlan


class IncidentPayload(BaseModel):
    incident_id: str | None = Field(default=None, alias="incidentId")
    type: str
    severity: str = "MEDIUM"
    title: str = ""
    job_id: str | None = Field(default=None, alias="jobId")
    job_type: str | None = Field(default=None, alias="jobType")
    evidence: str = ""
    source: str = "unknown"
    timestamp: datetime | None = None
    attributes: dict[str, str] = Field(default_factory=dict)
    plan: dict[str, Any] | None = None
    self_healer: SelfHealerOutput | None = None

    model_config = {"populate_by_name": True}

    @classmethod
    def from_self_healer(cls, output: SelfHealerOutput, source: str = "self-healer") -> "IncidentPayload":
        incident = output.incident
        return cls(
            type=incident.type,
            severity=incident.severity,
            title=incident.title,
            job_id=incident.job_id or None,
            job_type=incident.job_type or None,
            evidence=incident.evidence,
            source=source,
            plan=output.plan.model_dump(by_alias=True),
            self_healer=output,
        )


class AnalysisResult(BaseModel):
    root_cause: str
    impact: str
    recommended_action: str
    confidence: float = Field(ge=0.0, le=1.0)
    reasoning: str
    used_llm: bool = False
    strategy: str | None = None


class PlannedAction(BaseModel):
    action: str
    endpoint: str | None = None
    params: dict[str, Any] = Field(default_factory=dict)
    auto_execute: bool = True
    confidence: float = Field(ge=0.0, le=1.0)
    rationale: str
    strategy: str | None = None
    steps: list[str] = Field(default_factory=list)
    verification_checks: list[str] = Field(default_factory=list)


class ExecutionResult(BaseModel):
    action: str
    success: bool
    message: str
    details: list[str] = Field(default_factory=list)
    dry_run: bool = False


class AgentRunResult(BaseModel):
    incident_id: str
    incident_type: str
    analysis: AnalysisResult
    plan: PlannedAction
    execution: ExecutionResult | None = None
    escalated: bool = False
    self_healer_strategy: str | None = None
