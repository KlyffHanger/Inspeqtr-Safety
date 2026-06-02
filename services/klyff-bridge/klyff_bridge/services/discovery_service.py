"""Device discovery and pipeline payload resolution."""

from __future__ import annotations

import json
import socket
from typing import Any
from urllib.parse import urlparse

from klyff_bridge.clients.klyff_client import KlyffRestClient
from klyff_bridge.config.settings import AppSettings
from klyff_bridge.models.runtime_models import DeviceRuntimeConfig
from klyff_bridge.utils.data import coerce_bool, get_first, maybe_json_dict, safe_float, safe_int, slugify


class DeviceDiscoveryService:
    """Resolve desired runtime device configurations from KLYFF state."""

    def __init__(
        self,
        settings: AppSettings,
        klyff_client: KlyffRestClient,
        base_inventory: dict[str, Any],
        detection_config: dict[str, Any],
    ) -> None:
        self.settings = settings
        self.klyff_client = klyff_client
        self.base_inventory = base_inventory
        self.detection_config = detection_config

    def stream_validation_error(self, source_uri: str) -> str | None:
        """Validate a source URI and return an error message when invalid."""
        parsed = urlparse(source_uri)
        if not parsed.scheme:
            return "missing URI scheme"

        if parsed.scheme in {"rtsp", "rtsps"}:
            if not parsed.hostname:
                return "missing RTSP hostname"
            if not self.settings.default_validate_streams:
                return None
            port = parsed.port or (322 if parsed.scheme == "rtsps" else 554)
            try:
                with socket.create_connection(
                    (parsed.hostname, port),
                    timeout=self.settings.stream_connect_timeout_seconds,
                ):
                    return None
            except OSError as exc:
                return f"cannot reach {parsed.hostname}:{port} ({exc})"

        if parsed.scheme in {"file", "v4l2"}:
            return None

        return None

    def discover_devices(self) -> dict[str, DeviceRuntimeConfig]:
        """Fetch and resolve all desired managed devices from KLYFF."""
        devices = self.klyff_client.list_devices()
        default_device = self.base_inventory.get("default_device", "CPU")
        topology = self.base_inventory.get("topology", {})
        raw_candidates: list[dict[str, Any]] = []

        for device in devices:
            device_type = device.get("type")
            if (
                self.settings.klyff_device_type_filter
                and device_type not in self.settings.klyff_device_type_filter
            ):
                continue

            device_id_container = device.get("id")
            if not isinstance(device_id_container, dict):
                continue
            device_id = device_id_container.get("id")
            device_name = device.get("name")
            if not isinstance(device_id, str) or not isinstance(device_name, str):
                continue

            attrs = self.klyff_client.get_device_attributes(device_id)
            nvr_enabled = attrs.get("nvrEnabled")
            if self.settings.klyff_require_nvr_enabled and not coerce_bool(nvr_enabled, False):
                continue

            source_value = get_first(attrs, self.settings.device_source_uri_keys, "")
            source_uri = source_value.strip() if isinstance(source_value, str) else ""
            if not source_uri:
                continue

            enabled = coerce_bool(attrs.get("enabled"), True) and coerce_bool(nvr_enabled, True)
            raw_candidates.append(
                {
                    "device_id": device_id,
                    "name": device_name,
                    "type": device_type or "",
                    "attrs": attrs,
                    "source_uri": source_uri,
                    "enabled": enabled,
                    "default_device": default_device,
                    "topology": topology,
                }
            )

        shared_count = sum(
            1
            for candidate in raw_candidates
            if candidate["enabled"] and coerce_bool(candidate["attrs"].get("useSharedPipeline"), True)
        )

        desired: dict[str, DeviceRuntimeConfig] = {}
        for candidate in raw_candidates:
            attrs = candidate["attrs"]
            name_slug = slugify(candidate["name"])
            metadata_topic = get_first(
                attrs,
                self.settings.device_metadata_topic_keys,
                f"{self.settings.default_metadata_topic_prefix}{name_slug}",
            )
            peer_id = get_first(
                attrs,
                self.settings.device_peer_id_keys,
                f"{self.settings.default_peer_id_prefix}{name_slug}",
            )
            pipeline = get_first(
                attrs,
                self.settings.device_pipeline_keys,
                candidate["topology"].get("pipeline", "worker_safety_gear_detection_mqtt"),
            )
            source_type = attrs.get("sourceType", attrs.get("source_type", self.settings.default_source_type))
            publish_frame = coerce_bool(
                attrs.get("publishFrame", attrs.get("publish_frame", False)),
                False,
            )
            use_shared_pipeline = coerce_bool(
                attrs.get("useSharedPipeline", attrs.get("use_shared_pipeline", True)),
                True,
            )

            base_model_instance_id = str(
                attrs.get("modelInstanceId")
                or attrs.get("model_instance_id")
                or candidate["topology"].get("model_instance_id", "instnvr0")
            )
            if use_shared_pipeline and candidate["topology"].get("mode") == "shared_nvr":
                model_instance_id = base_model_instance_id
                batch_size = candidate["topology"].get("batch_size", "auto")
                if batch_size == "auto":
                    batch_size = shared_count
            else:
                model_instance_id = f"{base_model_instance_id}_{name_slug}"
                batch_size = 1

            pipeline_detection_properties: dict[str, Any] = {
                "model-instance-id": model_instance_id,
                "batch-size": batch_size,
                "nireq": safe_int(attrs.get("nireq")) or candidate["topology"].get("nireq", 2),
                "inference-interval": (
                    safe_int(attrs.get("inferenceInterval"))
                    or safe_int(attrs.get("inference_interval"))
                    or candidate["topology"].get("inference_interval", 1)
                ),
            }

            threshold = safe_float(attrs.get("threshold"))
            if threshold is None:
                threshold = safe_float(candidate["topology"].get("threshold"))
            if threshold is not None:
                pipeline_detection_properties["threshold"] = threshold

            detection_properties = {
                "device": attrs.get("device", candidate["default_device"]),
                **self.detection_config,
            }
            detection_properties.update(pipeline_detection_properties)
            topology_detection_properties = candidate["topology"].get("detection_properties", {})
            if isinstance(topology_detection_properties, dict):
                detection_properties.update(topology_detection_properties)
            detection_properties.update(maybe_json_dict(attrs.get("detectionProperties")))

            payload = self.build_pipeline_payload(
                source_uri=candidate["source_uri"],
                source_type=str(source_type),
                metadata_topic=str(metadata_topic),
                peer_id=str(peer_id),
                publish_frame=publish_frame,
                detection_properties=detection_properties,
            )
            config_hash = json.dumps(
                {"pipeline": pipeline, "payload": payload, "enabled": candidate["enabled"]},
                sort_keys=True,
                separators=(",", ":"),
            )

            desired[candidate["device_id"]] = DeviceRuntimeConfig(
                device_id=candidate["device_id"],
                name=candidate["name"],
                type=candidate["type"],
                enabled=candidate["enabled"],
                source_uri=candidate["source_uri"],
                metadata_topic=str(metadata_topic),
                peer_id=str(peer_id),
                pipeline=str(pipeline),
                source_type=str(source_type),
                publish_frame=publish_frame,
                default_device=str(candidate["default_device"]),
                detection_properties=detection_properties,
                use_shared_pipeline=use_shared_pipeline,
                config_hash=config_hash,
            )

        return desired

    def build_pipeline_payload(
        self,
        *,
        source_uri: str,
        source_type: str,
        metadata_topic: str,
        peer_id: str,
        publish_frame: bool,
        detection_properties: dict[str, Any],
    ) -> dict[str, Any]:
        """Build the DLStreamer pipeline payload for one device."""
        return {
            "source": {"uri": source_uri, "type": source_type},
            "destination": {
                "metadata": {
                    "type": "mqtt",
                    "publish_frame": publish_frame,
                    "topic": metadata_topic,
                },
                "frame": {"type": "webrtc", "peer-id": peer_id},
            },
            "parameters": {"detection-properties": detection_properties},
        }
