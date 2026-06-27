import structlog

from app.clients.job_queue import JobQueueClient
from app.config import settings
from app.models import ExecutionResult, PlannedAction

log = structlog.get_logger()


class ExecutorAgent:
    def __init__(self, client: JobQueueClient) -> None:
        self.client = client

    def execute(self, plan: PlannedAction) -> ExecutionResult:
        if settings.dry_run:
            return ExecutionResult(
                action=plan.action,
                success=True,
                message="Dry-run mode: action not executed",
                details=[f"Would call {plan.endpoint}", *plan.steps[:2]],
                dry_run=True,
            )

        if plan.action == "VERIFY_RETRY_OR_FINAL_STATE":
            return self._verify_job_state(plan)

        if plan.action in ("WAIT_FOR_RETRY_WINDOW", "PAUSE_AND_ESCALATE_JOB_TYPE", "ALERT_OPERATOR"):
            return ExecutionResult(
                action=plan.action,
                success=True,
                message="Monitoring per self-healer strategy; no mutation applied",
                details=plan.steps or [plan.rationale],
            )

        if not plan.auto_execute or not plan.endpoint:
            return ExecutionResult(
                action=plan.action,
                success=True,
                message="No auto-execution required",
                details=[plan.rationale],
            )

        try:
            result = self._dispatch(plan)
            return ExecutionResult(
                action=plan.action,
                success=result.get("success", True),
                message=result.get("message", "Action completed"),
                details=(result.get("details") or []) + plan.verification_checks[:2],
            )
        except Exception as exc:
            log.error("executor.failed", action=plan.action, error=str(exc))
            return ExecutionResult(action=plan.action, success=False, message=str(exc))

    def _verify_job_state(self, plan: PlannedAction) -> ExecutionResult:
        job_id = plan.params.get("job_id")
        if not job_id:
            return ExecutionResult(
                action=plan.action,
                success=True,
                message="No jobId to verify",
                details=plan.steps,
            )

        job = self.client.get_job(str(job_id))
        if job is None:
            return ExecutionResult(
                action=plan.action,
                success=False,
                message=f"Job {job_id} not found",
                details=plan.verification_checks,
            )

        status = job.get("status", "UNKNOWN")
        ok = status in ("RETRY", "FAILED", "COMPLETED")
        return ExecutionResult(
            action=plan.action,
            success=ok,
            message=f"Verified job {job_id} status={status}",
            details=plan.verification_checks + [f"observed_status={status}"],
        )

    def _dispatch(self, plan: PlannedAction) -> dict:
        endpoint = plan.endpoint or ""
        if endpoint == "/admin/watcher/recover":
            return self.client.recover_watcher()
        if endpoint == "/admin/redis/reconcile":
            return self.client.reconcile_redis()
        if endpoint.startswith("/admin/jobs/") and endpoint.endswith("/requeue"):
            job_id = plan.params.get("job_id") or endpoint.split("/")[3]
            return self.client.requeue_job(str(job_id))
        if endpoint.startswith("/admin/circuit/") and endpoint.endswith("/reset"):
            job_type = plan.params.get("job_type") or endpoint.split("/")[3]
            return self.client.reset_circuit(str(job_type))
        raise ValueError(f"Unknown endpoint: {endpoint}")
