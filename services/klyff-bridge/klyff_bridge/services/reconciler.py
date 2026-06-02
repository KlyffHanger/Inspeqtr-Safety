"""Pipeline reconciliation against desired device state."""

from __future__ import annotations

import logging
from collections.abc import Callable
from typing import Any

from klyff_bridge.clients.pipeline_client import PipelineController
from klyff_bridge.models.runtime_models import ManagedState
from klyff_bridge.services.discovery_service import DeviceDiscoveryService
from klyff_bridge.utils.logging import structured_log


StatusUpdater = Callable[[str, str, str, ManagedState | None], None]
StateSaver = Callable[[], None]


class ReconciliationService:
    """Reconcile desired device state with running pipeline instances."""

    def __init__(
        self,
        pipeline_controller: PipelineController,
        discovery_service: DeviceDiscoveryService,
        logger: logging.Logger,
        status_updater: StatusUpdater,
        state_saver: StateSaver,
    ) -> None:
        self.pipeline_controller = pipeline_controller
        self.discovery_service = discovery_service
        self.logger = logger
        self.status_updater = status_updater
        self.state_saver = state_saver

    def stop_device_if_running(
        self,
        device_id: str,
        state: ManagedState | None,
        running_ids: set[str],
    ) -> ManagedState:
        """Stop a running pipeline instance for a device when present."""
        next_state = state or ManagedState()
        instance_id = next_state.instance_id
        if instance_id and instance_id in running_ids:
            try:
                self.pipeline_controller.stop_pipeline(instance_id)
                structured_log(
                    self.logger,
                    logging.INFO,
                    "pipeline_stopped",
                    device_id=device_id,
                    instance_id=instance_id,
                )
            except Exception as exc:  # noqa: BLE001
                structured_log(
                    self.logger,
                    logging.ERROR,
                    "pipeline_stop_failed",
                    device_id=device_id,
                    instance_id=instance_id,
                    error=str(exc),
                )
                next_state.last_status = "stop_failed"
                next_state.last_error = str(exc)
                self.status_updater(device_id, "stop_failed", str(exc), next_state)
                return next_state

        next_state.instance_id = None
        return next_state

    def reconcile_once(self, state: dict[str, ManagedState]) -> dict[str, Any]:
        """Apply one full reconciliation pass and return desired devices."""
        desired_devices = self.discovery_service.discover_devices()
        running_ids = self.pipeline_controller.running_instance_ids()
        desired_ids = set(desired_devices)

        for device_id in list(state):
            if device_id in desired_ids:
                continue
            removed_state = self.stop_device_if_running(device_id, state.get(device_id), running_ids)
            removed_state.last_status = "removed"
            removed_state.last_error = None
            self.status_updater(
                device_id,
                "removed",
                "Device is no longer managed by the NVR sync service",
                removed_state,
            )
            state.pop(device_id, None)

        for device_id, device in desired_devices.items():
            device_state = state.get(device_id, ManagedState())
            device_state.metadata_topic = device.metadata_topic
            device_state.pipeline = device.pipeline
            device_state.peer_id = device.peer_id
            device_state.source_uri = device.source_uri

            if not device.enabled:
                device_state = self.stop_device_if_running(device_id, device_state, running_ids)
                device_state.last_status = "disabled"
                device_state.last_error = None
                state[device_id] = device_state
                self.status_updater(device_id, "disabled", "Device is disabled in KLYFF", device_state)
                continue

            if not device.config_hash:
                device_state.last_status = "invalid"
                device_state.last_error = "missing config hash"
                state[device_id] = device_state
                self.status_updater(device_id, "invalid", device_state.last_error, device_state)
                continue

            if not device.source_uri:
                device_state = self.stop_device_if_running(device_id, device_state, running_ids)
                device_state.last_status = "invalid"
                device_state.last_error = "missing source URI"
                state[device_id] = device_state
                self.status_updater(device_id, "invalid", device_state.last_error, device_state)
                continue

            validation_error = self.discovery_service.stream_validation_error(device.source_uri)
            if validation_error:
                device_state = self.stop_device_if_running(device_id, device_state, running_ids)
                device_state.last_status = "stream_error"
                device_state.last_error = validation_error
                state[device_id] = device_state
                self.status_updater(device_id, "stream_error", validation_error, device_state)
                structured_log(
                    self.logger,
                    logging.WARNING,
                    "stream_validation_failed",
                    device_id=device_id,
                    source_uri=device.source_uri,
                    error=validation_error,
                )
                continue

            instance_missing = not device_state.instance_id or device_state.instance_id not in running_ids
            config_changed = device_state.config_hash != device.config_hash

            if config_changed or instance_missing:
                if device_state.instance_id and device_state.instance_id in running_ids:
                    device_state = self.stop_device_if_running(device_id, device_state, running_ids)
                payload = self.discovery_service.build_pipeline_payload(
                    source_uri=device.source_uri,
                    source_type=device.source_type,
                    metadata_topic=device.metadata_topic,
                    peer_id=device.peer_id,
                    publish_frame=device.publish_frame,
                    detection_properties=device.detection_properties,
                )
                try:
                    instance_id = self.pipeline_controller.start_pipeline(device.pipeline, payload)
                except Exception as exc:  # noqa: BLE001
                    device_state.last_status = "start_failed"
                    device_state.last_error = str(exc)
                    state[device_id] = device_state
                    self.status_updater(device_id, "start_failed", str(exc), device_state)
                    structured_log(
                        self.logger,
                        logging.ERROR,
                        "pipeline_start_failed",
                        device_id=device_id,
                        pipeline=device.pipeline,
                        error=str(exc),
                    )
                    continue

                device_state.instance_id = instance_id
                device_state.config_hash = device.config_hash
                device_state.last_status = "running"
                device_state.last_error = None
                running_ids.add(instance_id)
                structured_log(
                    self.logger,
                    logging.INFO,
                    "pipeline_started",
                    device_id=device_id,
                    pipeline=device.pipeline,
                    instance_id=instance_id,
                )
            else:
                device_state.last_status = "running"
                device_state.last_error = None

            state[device_id] = device_state
            self.status_updater(device_id, "running", "Pipeline is synchronized", device_state)

        self.state_saver()
        structured_log(
            self.logger,
            logging.INFO,
            "reconcile_completed",
            managed_devices=len(desired_devices),
            state_entries=len(state),
        )
        return desired_devices
