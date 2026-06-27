import httpx
import structlog
from tenacity import retry, stop_after_attempt, wait_exponential

from app.config import settings

log = structlog.get_logger()


class JobQueueClient:
    def __init__(self) -> None:
        self.base_url = settings.job_queue_base_url.rstrip("/")
        self.headers = {"X-Admin-Api-Key": settings.admin_api_key}

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=0.5, min=0.5, max=4))
    def _post(self, path: str) -> dict:
        with httpx.Client(timeout=15.0) as client:
            response = client.post(f"{self.base_url}{path}", headers=self.headers)
            response.raise_for_status()
            return response.json()

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=0.5, min=0.5, max=4))
    def _get(self, path: str) -> dict:
        with httpx.Client(timeout=15.0) as client:
            response = client.get(f"{self.base_url}{path}", headers=self.headers)
            response.raise_for_status()
            return response.json()

    def recover_watcher(self) -> dict:
        log.info("admin.recover_watcher")
        return self._post("/admin/watcher/recover")

    def reconcile_redis(self) -> dict:
        log.info("admin.reconcile_redis")
        return self._post("/admin/redis/reconcile")

    def requeue_job(self, job_id: str) -> dict:
        log.info("admin.requeue_job", job_id=job_id)
        return self._post(f"/admin/jobs/{job_id}/requeue")

    def reset_circuit(self, job_type: str) -> dict:
        log.info("admin.reset_circuit", job_type=job_type)
        return self._post(f"/admin/circuit/{job_type}/reset")

    def get_detailed_health(self) -> dict:
        return self._get("/admin/health/detailed")

    def get_job(self, job_id: str) -> dict | None:
        with httpx.Client(timeout=10.0) as client:
            response = client.get(f"{self.base_url}/jobs/{job_id}")
            if response.status_code == 404:
                return None
            response.raise_for_status()
            return response.json()

    def get_stats(self) -> dict:
        with httpx.Client(timeout=10.0) as client:
            response = client.get(f"{self.base_url}/jobs/stats")
            response.raise_for_status()
            return response.json()
