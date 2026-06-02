"""MQTT subscription and telemetry forwarding components."""

from __future__ import annotations

import json
import logging
import time
from collections.abc import Callable
from typing import Any

import paho.mqtt.client as mqtt

from klyff_bridge.clients.klyff_client import KlyffRestClient
from klyff_bridge.config.settings import AppSettings
from klyff_bridge.models.runtime_models import DeviceRuntimeConfig, ManagedState
from klyff_bridge.utils.data import safe_float, slugify, trim_string
from klyff_bridge.utils.logging import structured_log


StatusUpdater = Callable[[str, str, str, ManagedState | None], None]


class TelemetryService:
    """Manage MQTT subscriptions and forward telemetry into KLYFF."""

    def __init__(
        self,
        settings: AppSettings,
        klyff_client: KlyffRestClient,
        logger: logging.Logger,
        status_updater: StatusUpdater,
    ) -> None:
        self.settings = settings
        self.klyff_client = klyff_client
        self.logger = logger
        self.status_updater = status_updater

    def on_mqtt_connect(self, client: mqtt.Client, reason_code: Any, topics: list[str]) -> None:
        """Restore MQTT subscriptions after a successful broker connection."""
        if reason_code != 0:
            structured_log(
                self.logger,
                logging.ERROR,
                "local_broker_connect_failed",
                host=self.settings.local_mqtt_host,
                port=self.settings.local_mqtt_port,
                reason=str(reason_code),
            )
            return

        for topic in topics:
            client.subscribe(topic, qos=self.settings.local_mqtt_qos)

        structured_log(
            self.logger,
            logging.INFO,
            "local_broker_connected",
            host=self.settings.local_mqtt_host,
            port=self.settings.local_mqtt_port,
            subscribed_topics=topics,
            qos=self.settings.local_mqtt_qos,
        )

    def update_subscriptions(
        self,
        mqtt_client: mqtt.Client,
        current_topics: dict[str, str],
        desired_devices: dict[str, DeviceRuntimeConfig],
    ) -> dict[str, str]:
        """Reconcile MQTT topic subscriptions with the desired device set."""
        desired_topics = {
            config.metadata_topic: config.device_id
            for config in desired_devices.values()
            if config.enabled
        }
        current_topic_names = set(current_topics)
        next_topic_names = set(desired_topics)
        to_add = sorted(next_topic_names - current_topic_names)
        to_remove = sorted(current_topic_names - next_topic_names)

        for topic in to_remove:
            mqtt_client.unsubscribe(topic)
        for topic in to_add:
            mqtt_client.subscribe(topic, qos=self.settings.local_mqtt_qos)

        if to_add or to_remove:
            structured_log(
                self.logger,
                logging.INFO,
                "mqtt_topics_reconciled",
                subscribed=sorted(next_topic_names),
                added=to_add,
                removed=to_remove,
            )

        return desired_topics

    def handle_message(
        self,
        *,
        topic: str,
        payload_bytes: bytes,
        device: DeviceRuntimeConfig | None,
        latest_fps: dict[str, float],
        current_state: ManagedState | None,
    ) -> None:
        """Forward one MQTT inference payload to KLYFF telemetry."""
        if device is None:
            structured_log(self.logger, logging.WARNING, "message_dropped_no_route", topic=topic)
            return

        payload = payload_bytes.decode("utf-8", errors="replace")
        try:
            parsed = json.loads(payload)
        except json.JSONDecodeError:
            parsed = None

        ts_ms = self._extract_timestamp_ms(parsed)
        values = self._flatten_payload(parsed)
        prefix = self.settings.klyff_timeseries_key_prefix
        values[f"{prefix}raw_payload"] = trim_string(payload, self.settings.klyff_telemetry_max_len)
        values[f"{prefix}topic"] = topic

        try:
            self.klyff_client.save_timeseries(device.device_id, values, ts_ms=ts_ms)
            self.klyff_client.save_server_attributes(
                device.device_id,
                {
                    "nvrLastTelemetryTs": int(time.time() * 1000),
                    "nvrLastTelemetryTopic": topic,
                    "nvrLastTelemetryStatus": "forwarded",
                },
            )
        except Exception as exc:  # noqa: BLE001
            structured_log(
                self.logger,
                logging.ERROR,
                "telemetry_forward_failed",
                device_id=device.device_id,
                topic=topic,
                error=str(exc),
            )
            self.status_updater(device.device_id, "telemetry_error", str(exc), current_state)
            return

        device_fps = self._extract_avg_fps(parsed)
        total_fps = None
        if device_fps is not None:
            latest_fps[device.device_id] = device_fps
            total_fps = round(sum(latest_fps.values()), 3)

        structured_log(
            self.logger,
            logging.INFO,
            "telemetry_forwarded",
            device_id=device.device_id,
            device_name=device.name,
            topic=topic,
            key_count=len(values),
            camera_fps=round(device_fps, 3) if device_fps is not None else None,
            total_fps=total_fps,
        )

    def _extract_timestamp_ms(self, payload: Any) -> int | None:
        if not isinstance(payload, dict):
            return None
        metadata = payload.get("metadata")
        if not isinstance(metadata, dict):
            return None
        timestamp_ns = metadata.get("time")
        if isinstance(timestamp_ns, int):
            return int(timestamp_ns / 1_000_000)
        timestamp = metadata.get("timestamp")
        if isinstance(timestamp, int):
            return timestamp
        return None

    def _extract_avg_fps(self, payload: Any) -> float | None:
        if not isinstance(payload, dict):
            return None
        metadata = payload.get("metadata")
        if not isinstance(metadata, dict):
            return None
        pipeline = metadata.get("pipeline")
        if not isinstance(pipeline, dict):
            return None
        status = pipeline.get("status")
        if not isinstance(status, dict):
            return None
        return safe_float(status.get("avg_fps"))

    def _flatten_payload(self, payload: Any) -> dict[str, Any]:
        flattened: dict[str, Any] = {}

        def visit(prefix: str, value: Any) -> None:
            key_prefix = self.settings.klyff_timeseries_key_prefix
            key = f"{key_prefix}{prefix}" if prefix else key_prefix.rstrip("_")
            if isinstance(value, bool):
                flattened[key] = value
                return
            if isinstance(value, (int, float, str)):
                flattened[key] = value
                return
            if isinstance(value, dict):
                for child_key, child_value in value.items():
                    if not isinstance(child_key, str):
                        continue
                    next_prefix = f"{prefix}_{slugify(child_key)}" if prefix else slugify(child_key)
                    visit(next_prefix, child_value)
                return
            if isinstance(value, list):
                flattened[f"{key}_count"] = len(value)
                label_counts: dict[str, int] = {}
                for item in value:
                    if not isinstance(item, dict):
                        continue
                    label = item.get("label") or item.get("class") or item.get("type")
                    if isinstance(label, str) and label:
                        label_key = slugify(label)
                        label_counts[label_key] = label_counts.get(label_key, 0) + 1
                for label_key, count in label_counts.items():
                    flattened[f"{key}_{label_key}_count"] = count
                return
            if value is not None:
                flattened[key] = trim_string(
                    json.dumps(value),
                    self.settings.klyff_telemetry_max_len,
                )

        if isinstance(payload, dict):
            visit("", payload)
        else:
            flattened[f"{self.settings.klyff_timeseries_key_prefix}payload_text"] = trim_string(
                str(payload),
                self.settings.klyff_telemetry_max_len,
            )

        return flattened
