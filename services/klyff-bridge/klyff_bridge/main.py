"""Application entrypoint for the KLYFF bridge service."""

from klyff_bridge.config.settings import AppSettings
from klyff_bridge.services.orchestrator import NvrOrchestrationService


def main() -> int:
    """Entrypoint for the NVR orchestration service."""
    return NvrOrchestrationService(AppSettings()).run()
