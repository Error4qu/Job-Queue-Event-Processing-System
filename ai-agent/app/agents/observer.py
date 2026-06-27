import json
import threading
import time
from collections import OrderedDict

import structlog
from confluent_kafka import Consumer, KafkaError

from app.config import settings
from app.models import IncidentPayload

log = structlog.get_logger()


class ObserverAgent:
    """Consumes incidents from Kafka with deduplication."""

    def __init__(self, on_incident) -> None:
        self.on_incident = on_incident
        self._seen: OrderedDict[str, float] = OrderedDict()
        self._thread: threading.Thread | None = None
        self._running = False

    def start(self) -> None:
        if self._running:
            return
        self._running = True
        self._thread = threading.Thread(target=self._consume_loop, daemon=True)
        self._thread.start()
        log.info("observer.started", topic=settings.incident_topic)

    def stop(self) -> None:
        self._running = False

    def _consume_loop(self) -> None:
        consumer = Consumer({
            "bootstrap.servers": settings.kafka_bootstrap_servers,
            "group.id": settings.kafka_group_id,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": True,
        })
        consumer.subscribe([settings.incident_topic])

        while self._running:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    log.error("observer.kafka_error", error=str(msg.error()))
                continue
            try:
                payload = json.loads(msg.value().decode("utf-8"))
                incident = IncidentPayload.model_validate(payload)
                if self._is_duplicate(incident):
                    log.info("observer.duplicate_skipped", type=incident.type)
                    continue
                self.on_incident(incident)
            except Exception as exc:
                log.error("observer.parse_failed", error=str(exc))

        consumer.close()

    def _is_duplicate(self, incident: IncidentPayload) -> bool:
        key = f"{incident.type}:{incident.job_id}:{incident.evidence[:80]}"
        now = time.time()
        self._purge(now)
        if key in self._seen:
            return True
        self._seen[key] = now
        return False

    def _purge(self, now: float) -> None:
        ttl = settings.dedup_ttl_seconds
        while self._seen:
            oldest_key, oldest_time = next(iter(self._seen.items()))
            if now - oldest_time > ttl:
                self._seen.pop(oldest_key)
            else:
                break
