import json
import os
import subprocess
from pathlib import Path

import structlog

from app.config import settings
from app.models import IncidentPayload, SelfHealerOutput

log = structlog.get_logger()


class SelfHealerClient:
    """Invokes the C++ self-healer binary for detection and healing plans."""

    def __init__(self, binary_path: str | None = None) -> None:
        self.binary_path = binary_path or settings.self_healer_bin

    def is_available(self) -> bool:
        return settings.self_healer_enabled and Path(self.binary_path).is_file()

    def analyze_log_line(self, line: str) -> SelfHealerOutput | None:
        if not self.is_available():
            return None
        proc = subprocess.run(
            [self.binary_path],
            input=line if line.endswith("\n") else line + "\n",
            capture_output=True,
            text=True,
            timeout=5,
        )
        return self._parse_stdout(proc.stdout)

    def analyze_log_file(self, path: str, follow: bool = False) -> list[SelfHealerOutput]:
        if not self.is_available():
            return []
        args = [self.binary_path, "--log-file", path]
        if follow:
            args.append("--follow")
        proc = subprocess.run(args, capture_output=True, text=True, timeout=30)
        return self._parse_all(proc.stdout)

    def analyze_log_lines(self, lines: list[str]) -> list[SelfHealerOutput]:
        if not self.is_available():
            return []
        payload = "".join(line if line.endswith("\n") else line + "\n" for line in lines)
        proc = subprocess.run(
            [self.binary_path],
            input=payload,
            capture_output=True,
            text=True,
            timeout=15,
        )
        return self._parse_all(proc.stdout)

    def enrich_incident(self, incident: IncidentPayload) -> IncidentPayload:
        """Run evidence through self-healer when no plan is attached yet."""
        if incident.self_healer or incident.plan:
            return incident
        if not incident.evidence:
            return incident

        output = self.analyze_log_line(incident.evidence)
        if output is None:
            return incident

        enriched = IncidentPayload.from_self_healer(output, source=incident.source)
        enriched.incident_id = incident.incident_id
        enriched.timestamp = incident.timestamp
        enriched.attributes = incident.attributes
        return enriched

    def _parse_stdout(self, stdout: str) -> SelfHealerOutput | None:
        for line in stdout.splitlines():
            parsed = self._parse_line(line)
            if parsed is not None:
                return parsed
        return None

    def _parse_all(self, stdout: str) -> list[SelfHealerOutput]:
        results: list[SelfHealerOutput] = []
        for line in stdout.splitlines():
            parsed = self._parse_line(line)
            if parsed is not None:
                results.append(parsed)
        return results

    def _parse_line(self, line: str) -> SelfHealerOutput | None:
        line = line.strip()
        if not line.startswith("{"):
            return None
        try:
            data = json.loads(line)
            return SelfHealerOutput.model_validate(data)
        except (json.JSONDecodeError, ValueError) as exc:
            log.warning("self_healer.parse_failed", error=str(exc), line=line[:120])
            return None


def resolve_sample_log_path() -> str | None:
    """Locate bundled self-healer/examples/sample.txt for demos."""
    candidates = [
        Path("/app/self-healer/examples/sample.txt"),
        Path(__file__).resolve().parents[3] / "self-healer" / "examples" / "sample.txt",
    ]
    for path in candidates:
        if path.is_file():
            return str(path)
    return None
