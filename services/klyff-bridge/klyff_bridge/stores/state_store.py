"""State persistence for device synchronization."""

from __future__ import annotations

import json
import logging
from pathlib import Path

from klyff_bridge.models.runtime_models import ManagedState
from klyff_bridge.utils.logging import structured_log


class PipelineStateStore:
    """Load and save persisted pipeline synchronization state."""

    def __init__(self, state_file: Path, logger: logging.Logger) -> None:
        self.state_file = state_file
        self.logger = logger

    def load(self) -> dict[str, ManagedState]:
        """Load persisted state from disk."""
        if not self.state_file.exists():
            return {}
        try:
            payload = json.loads(self.state_file.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            structured_log(
                self.logger,
                logging.WARNING,
                "state_load_failed",
                state_file=str(self.state_file),
                error=str(exc),
            )
            return {}

        if not isinstance(payload, dict):
            return {}

        return {
            device_id: ManagedState.model_validate(value)
            for device_id, value in payload.items()
            if isinstance(device_id, str) and isinstance(value, dict)
        }

    def save(self, state: dict[str, ManagedState]) -> None:
        """Persist state to disk."""
        self.state_file.parent.mkdir(parents=True, exist_ok=True)
        serializable = {device_id: item.model_dump(mode="json") for device_id, item in state.items()}
        self.state_file.write_text(json.dumps(serializable, indent=2) + "\n", encoding="utf-8")
