"""Top-level orchestration shell for KLYFF-managed NVR pipelines."""

from __future__ import annotations

import logging
import signal
import threading
from typing import Any

import paho.mqtt.client as mqtt

from klyff_bridge.clients.pipeline_client import PipelineController
from klyff_bridge.clients.klyff_client import KlyffRestClient
from klyff_bridge.config.settings import AppSettings
from klyff_bridge.models.runtime_models import DeviceRuntimeConfig, ManagedState
from klyff_bridge.services.discovery_service import DeviceDiscoveryService
from klyff_bridge.services.reconciler import ReconciliationService
from klyff_bridge.services.telemetry_service import TelemetryService
from klyff_bridge.stores.state_store import PipelineStateStore
from klyff_bridge.utils.data import load_json
from klyff_bridge.utils.logging import configure_logging, structured_log


class NvrOrchestrationService:
    """Coordinate service lifecycle across discovery, telemetry, and reconciliation."""

    def __init__(self, settings: AppSettings) -> None:
        self.settings = settings
        self.logger = configure_logging(settings.log_level, settings.logger_name)
        self.stop_event = threading.Event()
        self.state_lock = threading.Lock()

        self.klyff_client = KlyffRestClient(settings)
        self.pipeline_controller = PipelineController(settings)
        self.state_store = PipelineStateStore(settings.pipeline_state_file, self.logger)

        self.topic_to_device_id: dict[str, str] = {}
        self.managed_devices: dict[str, DeviceRuntimeConfig] = {}
        self.latest_fps: dict[str, float] = {}
        self.state = self.state_store.load()

        self.mqtt_client = mqtt.Client(
            mqtt.CallbackAPIVersion.VERSION2,
            client_id=settings.mqtt_client_id,
        )
        self.mqtt_client.on_connect = self._on_mqtt_connect
        self.mqtt_client.on_message = self._on_mqtt_message

        self.base_inventory = load_json(settings.nvr_base_config_file)
        self.detection_config = load_json(settings.detection_config_file)

        self.discovery_service = DeviceDiscoveryService(
            settings=settings,
            klyff_client=self.klyff_client,
            base_inventory=self.base_inventory,
            detection_config=self.detection_config,
        )
        self.telemetry_service = TelemetryService(
            settings=settings,
            klyff_client=self.klyff_client,
            logger=self.logger,
            status_updater=self._update_device_status,
        )
        self.reconciliation_service = ReconciliationService(
            pipeline_controller=self.pipeline_controller,
            discovery_service=self.discovery_service,
            logger=self.logger,
            status_updater=self._update_device_status,
            state_saver=self._save_state,
        )

    def _log(self, level: int, message: str, **extra: Any) -> None:
        structured_log(self.logger, level, message, **extra)

    def _handle_signal(self, signum: int, _frame: Any) -> None:
        self._log(logging.INFO, "shutdown_signal_received", signal=signum)
        self.stop_event.set()

    def _save_state(self) -> None:
        self.state_store.save(self.state)

    def _on_mqtt_connect(
        self,
        client: mqtt.Client,
        _userdata: Any,
        _flags: Any,
        reason_code: Any,
        _properties: Any,
    ) -> None:
        with self.state_lock:
            topics = sorted(self.topic_to_device_id)
        self.telemetry_service.on_mqtt_connect(client=client, reason_code=reason_code, topics=topics)

    def _update_mqtt_subscriptions(self, desired_devices: dict[str, DeviceRuntimeConfig]) -> None:
        with self.state_lock:
            self.topic_to_device_id = self.telemetry_service.update_subscriptions(
                mqtt_client=self.mqtt_client,
                current_topics=self.topic_to_device_id,
                desired_devices=desired_devices,
            )
            self.managed_devices = desired_devices

    def _on_mqtt_message(self, _client: mqtt.Client, _userdata: Any, msg: mqtt.MQTTMessage) -> None:
        topic = msg.topic
        with self.state_lock:
            device_id = self.topic_to_device_id.get(topic)
            device = self.managed_devices.get(device_id) if device_id else None
            current_state = self.state.get(device_id) if device_id else None
            latest_fps = self.latest_fps

        self.telemetry_service.handle_message(
            topic=topic,
            payload_bytes=msg.payload,
            device=device,
            latest_fps=latest_fps,
            current_state=current_state,
        )

    def _update_device_status(
        self,
        device_id: str,
        status: str,
        message: str,
        state: ManagedState | None,
    ) -> None:
        return

    def reconcile_once(self) -> None:
        """Reconcile desired KLYFF state with running DLStreamer pipelines."""
        desired_devices = self.reconciliation_service.reconcile_once(self.state)
        self._update_mqtt_subscriptions(desired_devices)

    def run(self) -> int:
        """Run the long-lived orchestration service loop."""
        signal.signal(signal.SIGTERM, self._handle_signal)
        signal.signal(signal.SIGINT, self._handle_signal)

        self.mqtt_client.connect(
            self.settings.local_mqtt_host,
            self.settings.local_mqtt_port,
            keepalive=60,
        )
        self.mqtt_client.loop_start()

        self._log(
            logging.INFO,
            "dynamic_sync_starting",
            pipeline_server_url=self.settings.pipeline_server_url,
            klyff_api_base_url=self.settings.klyff_api_base_url,
            state_file=str(self.settings.pipeline_state_file),
            base_config_file=str(self.settings.nvr_base_config_file),
            detection_config_file=str(self.settings.detection_config_file),
            poll_seconds=self.settings.klyff_sync_poll_seconds,
        )

        try:
            while not self.stop_event.is_set():
                try:
                    self.reconcile_once()
                except Exception as exc:  # noqa: BLE001
                    self._log(logging.ERROR, "reconcile_failed", error=str(exc))
                self.stop_event.wait(self.settings.klyff_sync_poll_seconds)
        finally:
            self.stop_event.set()
            self.mqtt_client.loop_stop()
            self.mqtt_client.disconnect()
            self._save_state()
            self._log(logging.INFO, "dynamic_sync_stopped")

        return 0
