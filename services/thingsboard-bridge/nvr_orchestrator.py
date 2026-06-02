from __future__ import annotations

import json
import logging
import os
import signal
import socket
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from urllib.parse import quote, urlparse

import paho.mqtt.client as mqtt
import requests


def env(name: str, default: str | None = None, required: bool = False) -> str:
    value = os.getenv(name, default)
    if value == "":
        value = default
    if required and not value:
        raise RuntimeError(f"{name} is required")
    return value or ""


def env_int(name: str, default: int) -> int:
    return int(env(name, str(default)))


def env_float(name: str, default: float) -> float:
    return float(env(name, str(default)))


def env_bool(name: str, default: bool) -> bool:
    value = env(name, "true" if default else "false").strip().lower()
    return value in {"1", "true", "yes", "on"}


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "timestamp": self.formatTime(record, "%Y-%m-%dT%H:%M:%S"),
            "level": record.levelname,
            "message": record.getMessage(),
            "logger": record.name,
        }
        extra = getattr(record, "extra_fields", None)
        if isinstance(extra, dict):
            payload.update(extra)
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload)


logging.basicConfig(level=env("LOG_LEVEL", "INFO"), format="%(message)s")
logger = logging.getLogger("thingsboard_dynamic_sync")
for handler in logging.getLogger().handlers:
    handler.setFormatter(JsonFormatter())


PIPELINE_ROOT = env("PIPELINE_ROOT", "user_defined_pipelines")
PIPELINE_SERVER_URL = env("PIPELINE_SERVER_URL", "http://dlstreamer-pipeline-server:8080").rstrip("/")
PIPELINE_SERVER_API_PREFIX = "/" + env("PIPELINE_SERVER_API_PREFIX", "").strip().strip("/")
PIPELINE_SERVER_VERIFY_TLS = env_bool("PIPELINE_SERVER_VERIFY_TLS", False)
PIPELINE_REQUEST_TIMEOUT_SECONDS = env_float("PIPELINE_REQUEST_TIMEOUT_SECONDS", 15.0)

LOCAL_MQTT_HOST = env("LOCAL_MQTT_HOST", "mqtt-broker")
LOCAL_MQTT_PORT = env_int("LOCAL_MQTT_PORT", 1883)
LOCAL_MQTT_QOS = env_int("LOCAL_MQTT_QOS", 0)

THINGSBOARD_API_BASE_URL = env("THINGSBOARD_API_BASE_URL", required=True).rstrip("/")
THINGSBOARD_API_KEY = env("THINGSBOARD_API_KEY", "")
THINGSBOARD_USERNAME = env("THINGSBOARD_USERNAME", "")
THINGSBOARD_PASSWORD = env("THINGSBOARD_PASSWORD", "")
THINGSBOARD_DEVICE_PAGE_SIZE = env_int("THINGSBOARD_DEVICE_PAGE_SIZE", 100)
THINGSBOARD_DEVICE_TYPE_FILTER = {
    value.strip() for value in env("THINGSBOARD_DEVICE_TYPE_FILTER", "").split(",") if value.strip()
}
THINGSBOARD_REQUIRE_NVR_ENABLED = env_bool("THINGSBOARD_REQUIRE_NVR_ENABLED", True)
THINGSBOARD_SYNC_POLL_SECONDS = env_float("THINGSBOARD_SYNC_POLL_SECONDS", 15.0)
THINGSBOARD_TIMESERIES_KEY_PREFIX = env("THINGSBOARD_TIMESERIES_KEY_PREFIX", "nvr_")
THINGSBOARD_SERVER_ATTR_SCOPE = env("THINGSBOARD_SERVER_ATTR_SCOPE", "SERVER_SCOPE")
THINGSBOARD_TELEMETRY_MAX_LEN = env_int("THINGSBOARD_TELEMETRY_MAX_LEN", 4096)

NVR_BASE_CONFIG_FILE = Path(env("NVR_BASE_CONFIG_FILE", "/config/nvr-base-config.json"))
DETECTION_CONFIG_FILE = Path(env("DETECTION_CONFIG_FILE", "/config/detection-config.json"))
PIPELINE_STATE_FILE = Path(env("PIPELINE_STATE_FILE", "/runtime/thingsboard-device-sync-state.json"))

DEVICE_SOURCE_URI_KEYS = [key.strip() for key in env("NVR_SOURCE_URI_KEYS", "sourceUri,source_uri,rtspUrl,rtsp_url").split(",") if key.strip()]
DEVICE_METADATA_TOPIC_KEYS = [key.strip() for key in env("NVR_METADATA_TOPIC_KEYS", "metadataTopic,metadata_topic").split(",") if key.strip()]
DEVICE_PEER_ID_KEYS = [key.strip() for key in env("NVR_PEER_ID_KEYS", "peerId,peer_id").split(",") if key.strip()]
DEVICE_PIPELINE_KEYS = [key.strip() for key in env("NVR_PIPELINE_KEYS", "pipeline,pipelineName").split(",") if key.strip()]

DEFAULT_METADATA_TOPIC_PREFIX = env("NVR_METADATA_TOPIC_PREFIX", "worker_safety_predictions_")
DEFAULT_PEER_ID_PREFIX = env("NVR_PEER_ID_PREFIX", "worker_safety_rtsp_")
DEFAULT_SOURCE_TYPE = env("NVR_SOURCE_TYPE", "uri")
DEFAULT_VALIDATE_STREAMS = env_bool("NVR_VALIDATE_STREAMS", True)
STREAM_CONNECT_TIMEOUT_SECONDS = env_float("NVR_STREAM_CONNECT_TIMEOUT_SECONDS", 3.0)

stop_event = threading.Event()


def log(level: int, message: str, **extra: Any) -> None:
    logger.log(level, message, extra={"extra_fields": extra})


def coerce_bool(value: Any, default: bool = False) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return default
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "on"}
    return default


def load_json(path: Path) -> Any:
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def slugify(value: str) -> str:
    lowered = value.strip().lower()
    sanitized = "".join(char if char.isalnum() else "_" for char in lowered)
    while "__" in sanitized:
        sanitized = sanitized.replace("__", "_")
    return sanitized.strip("_") or "device"


def trim_string(value: str, limit: int) -> str:
    if len(value) <= limit:
        return value
    return value[: max(0, limit - 3)] + "..."


def get_first(attrs: dict[str, Any], keys: list[str], default: Any = None) -> Any:
    for key in keys:
        if key in attrs and attrs[key] not in ("", None):
            return attrs[key]
    return default


def maybe_json_dict(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            return {}
        if isinstance(parsed, dict):
            return parsed
    return {}


def safe_float(value: Any) -> float | None:
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str) and value.strip():
        try:
            return float(value)
        except ValueError:
            return None
    return None


def safe_int(value: Any) -> int | None:
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    if isinstance(value, float) and value.is_integer():
        return int(value)
    if isinstance(value, str) and value.strip():
        try:
            return int(float(value))
        except ValueError:
            return None
    return None


def handle_signal(signum: int, _frame: Any) -> None:
    log(logging.INFO, "shutdown_signal_received", signal=signum)
    stop_event.set()


@dataclass
class DeviceRuntimeConfig:
    device_id: str
    name: str
    type: str
    enabled: bool
    source_uri: str
    metadata_topic: str
    peer_id: str
    pipeline: str
    source_type: str
    publish_frame: bool
    default_device: str
    detection_properties: dict[str, Any]
    use_shared_pipeline: bool
    config_hash: str


@dataclass
class ManagedState:
    instance_id: str | None = None
    config_hash: str | None = None
    metadata_topic: str | None = None
    pipeline: str | None = None
    peer_id: str | None = None
    source_uri: str | None = None
    last_status: str | None = None
    last_error: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "instance_id": self.instance_id,
            "config_hash": self.config_hash,
            "metadata_topic": self.metadata_topic,
            "pipeline": self.pipeline,
            "peer_id": self.peer_id,
            "source_uri": self.source_uri,
            "last_status": self.last_status,
            "last_error": self.last_error,
        }

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "ManagedState":
        return cls(
            instance_id=payload.get("instance_id"),
            config_hash=payload.get("config_hash"),
            metadata_topic=payload.get("metadata_topic"),
            pipeline=payload.get("pipeline"),
            peer_id=payload.get("peer_id"),
            source_uri=payload.get("source_uri"),
            last_status=payload.get("last_status"),
            last_error=payload.get("last_error"),
        )


class PipelineController:
    def __init__(self) -> None:
        self.session = requests.Session()

    def _api_path(self, path: str) -> str:
        normalized = "/" + path.lstrip("/")
        if PIPELINE_SERVER_API_PREFIX == "/":
            return normalized
        return f"{PIPELINE_SERVER_API_PREFIX}{normalized}"

    def _request(self, method: str, path: str, **kwargs: Any) -> requests.Response:
        response = self.session.request(
            method,
            f"{PIPELINE_SERVER_URL}{self._api_path(path)}",
            timeout=PIPELINE_REQUEST_TIMEOUT_SECONDS,
            verify=PIPELINE_SERVER_VERIFY_TLS,
            **kwargs,
        )
        response.raise_for_status()
        return response

    def status(self) -> list[dict[str, Any]]:
        response = self._request("GET", "/pipelines/status")
        payload = response.json()
        return payload if isinstance(payload, list) else []

    def running_instance_ids(self) -> set[str]:
        running = set()
        for item in self.status():
            if item.get("state") == "RUNNING" and isinstance(item.get("id"), str):
                running.add(item["id"])
        return running

    def start_pipeline(self, pipeline_name: str, payload: dict[str, Any]) -> str:
        response = self._request(
            "POST",
            f"/pipelines/{quote(PIPELINE_ROOT, safe='')}/{quote(pipeline_name, safe='')}",
            json=payload,
            headers={"Content-Type": "application/json"},
        )
        try:
            parsed = response.json()
        except ValueError:
            parsed = response.text.strip()

        if isinstance(parsed, str):
            return parsed.strip('"')
        raise RuntimeError(f"Unexpected pipeline start response for {pipeline_name}: {parsed!r}")

    def stop_pipeline(self, instance_id: str) -> None:
        self._request("DELETE", f"/pipelines/{quote(instance_id, safe='')}")


class ThingsBoardRestClient:
    def __init__(self) -> None:
        self.session = requests.Session()
        self._auth_header: str | None = None

    def _ensure_auth(self) -> str:
        if THINGSBOARD_API_KEY:
            return f"ApiKey {THINGSBOARD_API_KEY}"

        if self._auth_header:
            return self._auth_header

        if not THINGSBOARD_USERNAME or not THINGSBOARD_PASSWORD:
            raise RuntimeError("Set THINGSBOARD_API_KEY or THINGSBOARD_USERNAME/THINGSBOARD_PASSWORD")

        response = self.session.post(
            f"{THINGSBOARD_API_BASE_URL}/api/auth/login",
            json={"username": THINGSBOARD_USERNAME, "password": THINGSBOARD_PASSWORD},
            timeout=PIPELINE_REQUEST_TIMEOUT_SECONDS,
        )
        response.raise_for_status()
        payload = response.json()
        token = payload.get("token")
        if not isinstance(token, str) or not token:
            raise RuntimeError("ThingsBoard login did not return a token")
        self._auth_header = f"Bearer {token}"
        return self._auth_header

    def _request(self, method: str, path: str, **kwargs: Any) -> requests.Response:
        headers = kwargs.pop("headers", {})
        headers["X-Authorization"] = self._ensure_auth()
        response = self.session.request(
            method,
            f"{THINGSBOARD_API_BASE_URL}{path}",
            headers=headers,
            timeout=PIPELINE_REQUEST_TIMEOUT_SECONDS,
            **kwargs,
        )
        if response.status_code == 401 and not THINGSBOARD_API_KEY:
            self._auth_header = None
            headers["X-Authorization"] = self._ensure_auth()
            response = self.session.request(
                method,
                f"{THINGSBOARD_API_BASE_URL}{path}",
                headers=headers,
                timeout=PIPELINE_REQUEST_TIMEOUT_SECONDS,
                **kwargs,
            )
        response.raise_for_status()
        return response

    def list_devices(self) -> list[dict[str, Any]]:
        devices: list[dict[str, Any]] = []
        page = 0
        while True:
            response = self._request(
                "GET",
                f"/api/tenant/devices?pageSize={THINGSBOARD_DEVICE_PAGE_SIZE}&page={page}",
            )
            payload = response.json()
            data = payload.get("data")
            if not isinstance(data, list) or not data:
                break
            devices.extend(item for item in data if isinstance(item, dict))
            total_pages = payload.get("totalPages")
            has_next = payload.get("hasNext")
            if has_next is False:
                break
            if isinstance(total_pages, int) and page + 1 >= total_pages:
                break
            page += 1
        return devices

    def get_device_attributes(self, device_id: str) -> dict[str, Any]:
        response = self._request(
            "GET",
            f"/api/plugins/telemetry/DEVICE/{quote(device_id, safe='')}/values/attributes",
        )
        payload = response.json()
        attrs: dict[str, Any] = {}
        if isinstance(payload, list):
            for entry in payload:
                if not isinstance(entry, dict):
                    continue
                key = entry.get("key")
                if isinstance(key, str):
                    attrs[key] = entry.get("value")
        return attrs

    def save_server_attributes(self, device_id: str, attrs: dict[str, Any]) -> None:
        self._request(
            "POST",
            f"/api/plugins/telemetry/DEVICE/{quote(device_id, safe='')}/attributes/{quote(THINGSBOARD_SERVER_ATTR_SCOPE, safe='')}",
            json=attrs,
            headers={"Content-Type": "application/json"},
        )

    def save_timeseries(self, device_id: str, values: dict[str, Any], ts_ms: int | None = None) -> None:
        payload: dict[str, Any] | list[dict[str, Any]]
        if ts_ms is None:
            payload = values
        else:
            payload = {"ts": ts_ms, "values": values}
        self._request(
            "POST",
            f"/api/plugins/telemetry/DEVICE/{quote(device_id, safe='')}/timeseries/ANY",
            json=payload,
            headers={"Content-Type": "application/json"},
        )


class DynamicThingsBoardSync:
    def __init__(self) -> None:
        self.tb_client = ThingsBoardRestClient()
        self.pipeline_controller = PipelineController()
        self.topic_to_device_id: dict[str, str] = {}
        self.managed_devices: dict[str, DeviceRuntimeConfig] = {}
        self.latest_fps: dict[str, float] = {}
        self.state = self._load_state()
        self.state_lock = threading.Lock()
        self.mqtt_client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id="thingsboard-dynamic-sync")
        self.mqtt_client.on_connect = self._on_mqtt_connect
        self.mqtt_client.on_message = self._on_mqtt_message
        self.base_inventory = load_json(NVR_BASE_CONFIG_FILE)
        self.detection_config = load_json(DETECTION_CONFIG_FILE)

    def _load_state(self) -> dict[str, ManagedState]:
        if not PIPELINE_STATE_FILE.exists():
            return {}
        try:
            payload = json.loads(PIPELINE_STATE_FILE.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            log(logging.WARNING, "state_load_failed", state_file=str(PIPELINE_STATE_FILE), error=str(exc))
            return {}
        if not isinstance(payload, dict):
            return {}
        return {
            device_id: ManagedState.from_dict(value)
            for device_id, value in payload.items()
            if isinstance(device_id, str) and isinstance(value, dict)
        }

    def _save_state(self) -> None:
        PIPELINE_STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
        serializable = {device_id: state.to_dict() for device_id, state in self.state.items()}
        PIPELINE_STATE_FILE.write_text(json.dumps(serializable, indent=2) + "\n", encoding="utf-8")

    def _on_mqtt_connect(self, client: mqtt.Client, _userdata: Any, _flags: Any, reason_code: Any, _properties: Any) -> None:
        if reason_code != 0:
            log(logging.ERROR, "local_broker_connect_failed", host=LOCAL_MQTT_HOST, port=LOCAL_MQTT_PORT, reason=str(reason_code))
            return
        with self.state_lock:
            topics = sorted(self.topic_to_device_id)
        for topic in topics:
            client.subscribe(topic, qos=LOCAL_MQTT_QOS)
        log(logging.INFO, "local_broker_connected", host=LOCAL_MQTT_HOST, port=LOCAL_MQTT_PORT, subscribed_topics=topics, qos=LOCAL_MQTT_QOS)

    def _update_mqtt_subscriptions(self, desired_devices: dict[str, DeviceRuntimeConfig]) -> None:
        desired_topics = {
            config.metadata_topic: config.device_id
            for config in desired_devices.values()
            if config.enabled
        }
        with self.state_lock:
            current_topics = set(self.topic_to_device_id)
            next_topics = set(desired_topics)
            to_add = sorted(next_topics - current_topics)
            to_remove = sorted(current_topics - next_topics)
            self.topic_to_device_id = desired_topics
            self.managed_devices = desired_devices

        for topic in to_remove:
            self.mqtt_client.unsubscribe(topic)
        for topic in to_add:
            self.mqtt_client.subscribe(topic, qos=LOCAL_MQTT_QOS)

        if to_add or to_remove:
            log(logging.INFO, "mqtt_topics_reconciled", subscribed=sorted(next_topics), added=to_add, removed=to_remove)

    def _on_mqtt_message(self, _client: mqtt.Client, _userdata: Any, msg: mqtt.MQTTMessage) -> None:
        topic = msg.topic
        with self.state_lock:
            device_id = self.topic_to_device_id.get(topic)
            device = self.managed_devices.get(device_id) if device_id else None

        if device is None:
            log(logging.WARNING, "message_dropped_no_route", topic=topic)
            return

        payload = msg.payload.decode("utf-8", errors="replace")
        try:
            parsed = json.loads(payload)
        except json.JSONDecodeError:
            parsed = None

        ts_ms = self._extract_timestamp_ms(parsed)
        values = self._flatten_payload(parsed)
        values[f"{THINGSBOARD_TIMESERIES_KEY_PREFIX}raw_payload"] = trim_string(payload, THINGSBOARD_TELEMETRY_MAX_LEN)
        values[f"{THINGSBOARD_TIMESERIES_KEY_PREFIX}topic"] = topic

        try:
            self.tb_client.save_timeseries(device.device_id, values, ts_ms=ts_ms)
            self.tb_client.save_server_attributes(
                device.device_id,
                {
                    "nvrLastTelemetryTs": int(time.time() * 1000),
                    "nvrLastTelemetryTopic": topic,
                    "nvrLastTelemetryStatus": "forwarded",
                },
            )
        except Exception as exc:  # noqa: BLE001
            log(logging.ERROR, "telemetry_forward_failed", device_id=device.device_id, topic=topic, error=str(exc))
            self._update_device_status(device.device_id, "telemetry_error", str(exc), self.state.get(device.device_id))
            return

        device_fps = self._extract_avg_fps(parsed)
        total_fps = None
        if device_fps is not None:
            with self.state_lock:
                self.latest_fps[device.device_id] = device_fps
                total_fps = round(sum(self.latest_fps.values()), 3)

        log(
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
        avg_fps = status.get("avg_fps")
        return safe_float(avg_fps)

    def _flatten_payload(self, payload: Any) -> dict[str, Any]:
        flattened: dict[str, Any] = {}

        def visit(prefix: str, value: Any) -> None:
            key = f"{THINGSBOARD_TIMESERIES_KEY_PREFIX}{prefix}" if prefix else THINGSBOARD_TIMESERIES_KEY_PREFIX.rstrip("_")
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
                flattened[key] = trim_string(json.dumps(value), THINGSBOARD_TELEMETRY_MAX_LEN)

        if isinstance(payload, dict):
            visit("", payload)
        else:
            flattened[f"{THINGSBOARD_TIMESERIES_KEY_PREFIX}payload_text"] = trim_string(str(payload), THINGSBOARD_TELEMETRY_MAX_LEN)

        return flattened

    def _stream_validation_error(self, source_uri: str) -> str | None:
        parsed = urlparse(source_uri)
        if not parsed.scheme:
            return "missing URI scheme"

        if parsed.scheme in {"rtsp", "rtsps"}:
            if not parsed.hostname:
                return "missing RTSP hostname"
            if not DEFAULT_VALIDATE_STREAMS:
                return None
            port = parsed.port or (322 if parsed.scheme == "rtsps" else 554)
            try:
                with socket.create_connection((parsed.hostname, port), timeout=STREAM_CONNECT_TIMEOUT_SECONDS):
                    return None
            except OSError as exc:
                return f"cannot reach {parsed.hostname}:{port} ({exc})"

        if parsed.scheme in {"file", "v4l2"}:
            return None

        return None

    def _discover_devices(self) -> dict[str, DeviceRuntimeConfig]:
        devices = self.tb_client.list_devices()
        default_device = self.base_inventory.get("default_device", "CPU")
        topology = self.base_inventory.get("topology", {})
        raw_candidates: list[dict[str, Any]] = []

        for device in devices:
            device_type = device.get("type")
            if THINGSBOARD_DEVICE_TYPE_FILTER and device_type not in THINGSBOARD_DEVICE_TYPE_FILTER:
                continue

            device_id_container = device.get("id")
            if not isinstance(device_id_container, dict):
                continue
            device_id = device_id_container.get("id")
            device_name = device.get("name")
            if not isinstance(device_id, str) or not isinstance(device_name, str):
                continue

            attrs = self.tb_client.get_device_attributes(device_id)
            nvr_enabled = attrs.get("nvrEnabled")
            if THINGSBOARD_REQUIRE_NVR_ENABLED and not coerce_bool(nvr_enabled, False):
                continue

            source_uri = get_first(attrs, DEVICE_SOURCE_URI_KEYS, "").strip() if isinstance(get_first(attrs, DEVICE_SOURCE_URI_KEYS, ""), str) else ""
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
            metadata_topic = get_first(attrs, DEVICE_METADATA_TOPIC_KEYS, f"{DEFAULT_METADATA_TOPIC_PREFIX}{name_slug}")
            peer_id = get_first(attrs, DEVICE_PEER_ID_KEYS, f"{DEFAULT_PEER_ID_PREFIX}{name_slug}")
            pipeline = get_first(attrs, DEVICE_PIPELINE_KEYS, candidate["topology"].get("pipeline", "worker_safety_gear_detection_mqtt"))
            source_type = attrs.get("sourceType", attrs.get("source_type", DEFAULT_SOURCE_TYPE))
            publish_frame = coerce_bool(attrs.get("publishFrame", attrs.get("publish_frame", False)), False)
            use_shared_pipeline = coerce_bool(attrs.get("useSharedPipeline", attrs.get("use_shared_pipeline", True)), True)

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
                "inference-interval": safe_int(attrs.get("inferenceInterval")) or safe_int(attrs.get("inference_interval")) or candidate["topology"].get("inference_interval", 1),
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

            payload = self._build_pipeline_payload(
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

    def _build_pipeline_payload(
        self,
        *,
        source_uri: str,
        source_type: str,
        metadata_topic: str,
        peer_id: str,
        publish_frame: bool,
        detection_properties: dict[str, Any],
    ) -> dict[str, Any]:
        return {
            "source": {
                "uri": source_uri,
                "type": source_type,
            },
            "destination": {
                "metadata": {
                    "type": "mqtt",
                    "publish_frame": publish_frame,
                    "topic": metadata_topic,
                },
                "frame": {
                    "type": "webrtc",
                    "peer-id": peer_id,
                },
            },
            "parameters": {
                "detection-properties": detection_properties,
            },
        }

    def _update_device_status(self, device_id: str, status: str, message: str, state: ManagedState | None) -> None:
        attrs = {
            "nvrManaged": status not in {"removed", "disabled"},
            "nvrSyncStatus": status,
            "nvrSyncMessage": trim_string(message, 512),
            "nvrLastSyncTs": int(time.time() * 1000),
        }
        if state and state.instance_id:
            attrs["nvrPipelineInstanceId"] = state.instance_id
        if state and state.metadata_topic:
            attrs["nvrMetadataTopic"] = state.metadata_topic
        if state and state.pipeline:
            attrs["nvrPipelineName"] = state.pipeline
        if state and state.peer_id:
            attrs["nvrPeerId"] = state.peer_id
        if state and state.source_uri:
            attrs["nvrSourceUri"] = state.source_uri
        try:
            self.tb_client.save_server_attributes(device_id, attrs)
        except Exception as exc:  # noqa: BLE001
            log(logging.WARNING, "device_status_update_failed", device_id=device_id, status=status, error=str(exc))

    def _stop_device_if_running(self, device_id: str, state: ManagedState | None, running_ids: set[str]) -> ManagedState:
        next_state = state or ManagedState()
        instance_id = next_state.instance_id
        if instance_id and instance_id in running_ids:
            try:
                self.pipeline_controller.stop_pipeline(instance_id)
                log(logging.INFO, "pipeline_stopped", device_id=device_id, instance_id=instance_id)
            except Exception as exc:  # noqa: BLE001
                log(logging.ERROR, "pipeline_stop_failed", device_id=device_id, instance_id=instance_id, error=str(exc))
                next_state.last_status = "stop_failed"
                next_state.last_error = str(exc)
                self._update_device_status(device_id, "stop_failed", str(exc), next_state)
                return next_state

        next_state.instance_id = None
        return next_state

    def reconcile_once(self) -> None:
        desired_devices = self._discover_devices()
        running_ids = self.pipeline_controller.running_instance_ids()
        desired_ids = set(desired_devices)

        for device_id in list(self.state):
            if device_id in desired_ids:
                continue
            state = self._stop_device_if_running(device_id, self.state.get(device_id), running_ids)
            state.last_status = "removed"
            state.last_error = None
            self._update_device_status(device_id, "removed", "Device is no longer managed by the NVR sync service", state)
            self.state.pop(device_id, None)

        self._update_mqtt_subscriptions(desired_devices)

        for device_id, device in desired_devices.items():
            state = self.state.get(device_id, ManagedState())
            state.metadata_topic = device.metadata_topic
            state.pipeline = device.pipeline
            state.peer_id = device.peer_id
            state.source_uri = device.source_uri

            if not device.enabled:
                state = self._stop_device_if_running(device_id, state, running_ids)
                state.last_status = "disabled"
                state.last_error = None
                self.state[device_id] = state
                self._update_device_status(device_id, "disabled", "Device is disabled in ThingsBoard", state)
                continue

            if not device.config_hash:
                state.last_status = "invalid"
                state.last_error = "missing config hash"
                self.state[device_id] = state
                self._update_device_status(device_id, "invalid", state.last_error, state)
                continue

            if not device.source_uri:
                state = self._stop_device_if_running(device_id, state, running_ids)
                state.last_status = "invalid"
                state.last_error = "missing source URI"
                self.state[device_id] = state
                self._update_device_status(device_id, "invalid", state.last_error, state)
                continue

            validation_error = self._stream_validation_error(device.source_uri)
            if validation_error:
                state = self._stop_device_if_running(device_id, state, running_ids)
                state.last_status = "stream_error"
                state.last_error = validation_error
                self.state[device_id] = state
                self._update_device_status(device_id, "stream_error", validation_error, state)
                log(logging.WARNING, "stream_validation_failed", device_id=device_id, source_uri=device.source_uri, error=validation_error)
                continue

            instance_missing = not state.instance_id or state.instance_id not in running_ids
            config_changed = state.config_hash != device.config_hash

            if config_changed or instance_missing:
                if state.instance_id and state.instance_id in running_ids:
                    state = self._stop_device_if_running(device_id, state, running_ids)
                payload = self._build_pipeline_payload(
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
                    state.last_status = "start_failed"
                    state.last_error = str(exc)
                    self.state[device_id] = state
                    self._update_device_status(device_id, "start_failed", str(exc), state)
                    log(logging.ERROR, "pipeline_start_failed", device_id=device_id, pipeline=device.pipeline, error=str(exc))
                    continue

                state.instance_id = instance_id
                state.config_hash = device.config_hash
                state.last_status = "running"
                state.last_error = None
                running_ids.add(instance_id)
                log(logging.INFO, "pipeline_started", device_id=device_id, pipeline=device.pipeline, instance_id=instance_id)
            else:
                state.last_status = "running"
                state.last_error = None

            self.state[device_id] = state
            self._update_device_status(device_id, "running", "Pipeline is synchronized", state)

        self._save_state()
        log(logging.INFO, "reconcile_completed", managed_devices=len(desired_devices), state_entries=len(self.state))

    def run(self) -> int:
        signal.signal(signal.SIGTERM, handle_signal)
        signal.signal(signal.SIGINT, handle_signal)

        self.mqtt_client.connect(LOCAL_MQTT_HOST, LOCAL_MQTT_PORT, keepalive=60)
        self.mqtt_client.loop_start()

        log(
            logging.INFO,
            "dynamic_sync_starting",
            pipeline_server_url=PIPELINE_SERVER_URL,
            thingsboard_api_base_url=THINGSBOARD_API_BASE_URL,
            state_file=str(PIPELINE_STATE_FILE),
            base_config_file=str(NVR_BASE_CONFIG_FILE),
            detection_config_file=str(DETECTION_CONFIG_FILE),
            poll_seconds=THINGSBOARD_SYNC_POLL_SECONDS,
        )

        try:
            while not stop_event.is_set():
                try:
                    self.reconcile_once()
                except Exception as exc:  # noqa: BLE001
                    log(logging.ERROR, "reconcile_failed", error=str(exc))
                stop_event.wait(THINGSBOARD_SYNC_POLL_SECONDS)
        finally:
            stop_event.set()
            self.mqtt_client.loop_stop()
            self.mqtt_client.disconnect()
            self._save_state()
            log(logging.INFO, "dynamic_sync_stopped")

        return 0


def main() -> int:
    return DynamicThingsBoardSync().run()
