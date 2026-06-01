import json
import logging
import os
import queue
import signal
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import paho.mqtt.client as mqtt


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
logger = logging.getLogger("thingsboard_bridge")
for handler in logging.getLogger().handlers:
    handler.setFormatter(JsonFormatter())


LOCAL_MQTT_HOST = env("LOCAL_MQTT_HOST", "mqtt-broker")
LOCAL_MQTT_PORT = env_int("LOCAL_MQTT_PORT", 1883)
LOCAL_MQTT_TOPIC = env("LOCAL_MQTT_TOPIC", "")
THINGSBOARD_HOST = env("THINGSBOARD_HOST", required=True)
THINGSBOARD_PORT = env_int("THINGSBOARD_PORT", 1883)
THINGSBOARD_TOKEN = env("THINGSBOARD_TOKEN", "")
THINGSBOARD_TOPIC = env("THINGSBOARD_TOPIC", "v1/devices/me/telemetry")
THINGSBOARD_ROUTES_FILE = env("THINGSBOARD_ROUTES_FILE", "")
MQTT_QOS = env_int("MQTT_QOS", 0)
QUEUE_MAXSIZE = env_int("QUEUE_MAXSIZE", 1)
RETRY_BACKOFF_SECONDS = env_float("RETRY_BACKOFF_SECONDS", 2.0)
PUBLISH_TIMEOUT_SECONDS = env_float("PUBLISH_TIMEOUT_SECONDS", 10.0)
MIN_PUBLISH_INTERVAL_SECONDS = env_float("MIN_PUBLISH_INTERVAL_SECONDS", 2.0)
ENABLE_TIMING_LOGS = env("ENABLE_TIMING_LOGS", "true").lower() == "true"

stop_event = threading.Event()


@dataclass
class QueuedMessage:
    payload: str
    received_at: float
    source_time_ns: int | None = None
    source_timestamp: int | None = None
    frame_id: int | None = None
    pipeline_instance_id: str | None = None
    pipeline_avg_fps: float | None = None


@dataclass
class RouteConfig:
    name: str
    local_topic: str
    thingsboard_topic: str
    thingsboard_token: str
    publish_queue: queue.Queue[QueuedMessage] = field(init=False)
    tb_client: mqtt.Client | None = field(default=None, init=False)
    worker: threading.Thread | None = field(default=None, init=False)

    def __post_init__(self) -> None:
        self.publish_queue = queue.Queue(maxsize=QUEUE_MAXSIZE)


route_configs: list[RouteConfig] = []


def log(level: int, message: str, **extra: Any) -> None:
    logger.log(level, message, extra={"extra_fields": extra})


def handle_signal(signum: int, _frame: Any) -> None:
    log(logging.INFO, "shutdown_signal_received", signal=signum)
    stop_event.set()


def configure_client(
    client_id: str,
    username: str | None = None,
    on_connect=None,
    on_disconnect=None,
    on_message=None,
    userdata: Any = None,
) -> mqtt.Client:
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=client_id)
    if username:
        client.username_pw_set(username)
    if userdata is not None:
        client.user_data_set(userdata)
    if on_connect:
        client.on_connect = on_connect
    if on_disconnect:
        client.on_disconnect = on_disconnect
    if on_message:
        client.on_message = on_message
    client.reconnect_delay_set(min_delay=1, max_delay=30)
    return client


def resolve_route_token(raw_route: dict[str, Any]) -> str:
    inline_token = raw_route.get("thingsboard_token")
    if isinstance(inline_token, str) and inline_token:
        return inline_token

    token_env = raw_route.get("thingsboard_token_env")
    if isinstance(token_env, str) and token_env:
        return env(token_env, required=True)

    if THINGSBOARD_TOKEN:
        return THINGSBOARD_TOKEN

    raise RuntimeError(f"Missing ThingsBoard token for route: {raw_route}")


def load_route_configs() -> list[RouteConfig]:
    if THINGSBOARD_ROUTES_FILE:
        routes_path = Path(THINGSBOARD_ROUTES_FILE)
        if not routes_path.is_file():
            raise RuntimeError(f"THINGSBOARD_ROUTES_FILE does not exist: {routes_path}")

        raw_routes = json.loads(routes_path.read_text(encoding="utf-8"))
        if not isinstance(raw_routes, list) or not raw_routes:
            raise RuntimeError("THINGSBOARD_ROUTES_FILE must contain a non-empty JSON array")

        routes: list[RouteConfig] = []
        for index, raw_route in enumerate(raw_routes, start=1):
            if not isinstance(raw_route, dict):
                raise RuntimeError(f"Invalid route entry at index {index}: {raw_route!r}")

            local_topic = raw_route.get("local_topic")
            if not isinstance(local_topic, str) or not local_topic:
                raise RuntimeError(f"Route at index {index} is missing local_topic")

            name = raw_route.get("name")
            if not isinstance(name, str) or not name:
                name = f"route_{index}"

            thingsboard_topic = raw_route.get("thingsboard_topic")
            if not isinstance(thingsboard_topic, str) or not thingsboard_topic:
                thingsboard_topic = THINGSBOARD_TOPIC

            routes.append(
                RouteConfig(
                    name=name,
                    local_topic=local_topic,
                    thingsboard_topic=thingsboard_topic,
                    thingsboard_token=resolve_route_token(raw_route),
                )
            )
        return routes

    if not LOCAL_MQTT_TOPIC:
        raise RuntimeError("LOCAL_MQTT_TOPIC is required when THINGSBOARD_ROUTES_FILE is not set")
    if not THINGSBOARD_TOKEN:
        raise RuntimeError("THINGSBOARD_TOKEN is required when THINGSBOARD_ROUTES_FILE is not set")

    return [
        RouteConfig(
            name=LOCAL_MQTT_TOPIC,
            local_topic=LOCAL_MQTT_TOPIC,
            thingsboard_topic=THINGSBOARD_TOPIC,
            thingsboard_token=THINGSBOARD_TOKEN,
        )
    ]


def on_local_connect(client: mqtt.Client, _userdata: Any, _flags: Any, reason_code: Any, _properties: Any) -> None:
    if reason_code != 0:
        log(logging.ERROR, "local_broker_connect_failed", host=LOCAL_MQTT_HOST, port=LOCAL_MQTT_PORT, reason=str(reason_code))
        return

    subscribed_topics: set[str] = set()
    for route in route_configs:
        if route.local_topic in subscribed_topics:
            continue
        client.subscribe(route.local_topic, qos=MQTT_QOS)
        subscribed_topics.add(route.local_topic)

    log(
        logging.INFO,
        "local_broker_connected",
        host=LOCAL_MQTT_HOST,
        port=LOCAL_MQTT_PORT,
        subscribed_topics=sorted(subscribed_topics),
        qos=MQTT_QOS,
    )


def on_local_disconnect(_client: mqtt.Client, _userdata: Any, _flags: Any, reason_code: Any, _properties: Any) -> None:
    log(logging.WARNING, "local_broker_disconnected", reason=str(reason_code))


def parse_queued_message(payload: str) -> QueuedMessage:
    queued_message = QueuedMessage(payload=payload, received_at=time.perf_counter())

    try:
        parsed_payload = json.loads(payload)
    except json.JSONDecodeError:
        parsed_payload = None

    if isinstance(parsed_payload, dict):
        metadata = parsed_payload.get("metadata")
        if isinstance(metadata, dict):
            source_time_ns = metadata.get("time")
            if isinstance(source_time_ns, int):
                queued_message.source_time_ns = source_time_ns

            source_timestamp = metadata.get("timestamp")
            if isinstance(source_timestamp, int):
                queued_message.source_timestamp = source_timestamp

            frame_id = metadata.get("frame_id")
            if isinstance(frame_id, int):
                queued_message.frame_id = frame_id

            pipeline = metadata.get("pipeline")
            if isinstance(pipeline, dict):
                pipeline_instance_id = pipeline.get("instance_id")
                if isinstance(pipeline_instance_id, str):
                    queued_message.pipeline_instance_id = pipeline_instance_id

                status = pipeline.get("status")
                if isinstance(status, dict):
                    pipeline_avg_fps = status.get("avg_fps")
                    if isinstance(pipeline_avg_fps, (int, float)):
                        queued_message.pipeline_avg_fps = float(pipeline_avg_fps)

    return queued_message


def find_route_for_topic(topic: str) -> RouteConfig | None:
    for route in route_configs:
        if mqtt.topic_matches_sub(route.local_topic, topic):
            return route
    return None


def enqueue_message(route: RouteConfig, queued_message: QueuedMessage, source_topic: str) -> None:
    try:
        route.publish_queue.put(queued_message, timeout=1)
        log(
            logging.INFO,
            "message_queued",
            route=route.name,
            source_topic=source_topic,
            thingsboard_topic=route.thingsboard_topic,
            queue_size=route.publish_queue.qsize(),
        )
    except queue.Full:
        try:
            route.publish_queue.get_nowait()
            route.publish_queue.task_done()
        except queue.Empty:
            pass

        try:
            route.publish_queue.put_nowait(queued_message)
            log(
                logging.WARNING,
                "message_replaced_queue_full",
                route=route.name,
                source_topic=source_topic,
                queue_size=route.publish_queue.qsize(),
            )
        except queue.Full:
            log(
                logging.ERROR,
                "message_dropped_queue_full",
                route=route.name,
                source_topic=source_topic,
                queue_size=route.publish_queue.qsize(),
            )


def on_local_message(_client: mqtt.Client, _userdata: Any, msg: mqtt.MQTTMessage) -> None:
    route = find_route_for_topic(msg.topic)
    if route is None:
        log(logging.WARNING, "message_dropped_no_route", source_topic=msg.topic)
        return

    payload = msg.payload.decode("utf-8", errors="replace")
    enqueue_message(route, parse_queued_message(payload), msg.topic)


def on_tb_connect(_client: mqtt.Client, userdata: Any, _flags: Any, reason_code: Any, _properties: Any) -> None:
    route_name = userdata.get("route_name") if isinstance(userdata, dict) else None
    thingsboard_topic = userdata.get("thingsboard_topic") if isinstance(userdata, dict) else THINGSBOARD_TOPIC
    if reason_code != 0:
        log(
            logging.ERROR,
            "thingsboard_connect_failed",
            route=route_name,
            host=THINGSBOARD_HOST,
            port=THINGSBOARD_PORT,
            reason=str(reason_code),
        )
        return
    log(
        logging.INFO,
        "thingsboard_connected",
        route=route_name,
        host=THINGSBOARD_HOST,
        port=THINGSBOARD_PORT,
        topic=thingsboard_topic,
        qos=MQTT_QOS,
    )


def on_tb_disconnect(_client: mqtt.Client, userdata: Any, _flags: Any, reason_code: Any, _properties: Any) -> None:
    route_name = userdata.get("route_name") if isinstance(userdata, dict) else None
    log(logging.WARNING, "thingsboard_disconnected", route=route_name, reason=str(reason_code))


def publish_worker(route: RouteConfig) -> None:
    if route.tb_client is None:
        raise RuntimeError(f"ThingsBoard client not initialized for route {route.name}")

    last_publish_time = 0.0
    while not stop_event.is_set():
        try:
            queued_message = route.publish_queue.get(timeout=1)
        except queue.Empty:
            continue

        drained = 0
        while True:
            try:
                queued_message = route.publish_queue.get_nowait()
                route.publish_queue.task_done()
                drained += 1
            except queue.Empty:
                break

        if drained:
            log(
                logging.INFO,
                "queue_drained_to_latest",
                route=route.name,
                dropped_messages=drained,
                queue_size=route.publish_queue.qsize(),
            )

        remaining_interval = MIN_PUBLISH_INTERVAL_SECONDS - (time.time() - last_publish_time)
        throttle_sleep_ms = 0.0
        if remaining_interval > 0:
            throttle_sleep_ms = remaining_interval * 1000.0
            time.sleep(remaining_interval)

        publish_started_at = time.perf_counter()
        publish_started_epoch_ns = time.time_ns()
        publish_attempts = 0
        while not stop_event.is_set():
            publish_attempts += 1
            info = route.tb_client.publish(route.thingsboard_topic, queued_message.payload, qos=MQTT_QOS)
            if MQTT_QOS == 0:
                published = True
            else:
                info.wait_for_publish(timeout=PUBLISH_TIMEOUT_SECONDS)
                published = info.is_published()

            if published and info.rc == mqtt.MQTT_ERR_SUCCESS:
                last_publish_time = time.time()
                publish_completed_at = time.perf_counter()
                publish_completed_epoch_ns = time.time_ns()
                log_fields: dict[str, Any] = {
                    "route": route.name,
                    "topic": route.thingsboard_topic,
                    "local_topic": route.local_topic,
                    "queue_size": route.publish_queue.qsize(),
                }
                if queued_message.frame_id is not None:
                    log_fields["frame_id"] = queued_message.frame_id
                if queued_message.pipeline_instance_id:
                    log_fields["pipeline_instance_id"] = queued_message.pipeline_instance_id
                if queued_message.pipeline_avg_fps is not None:
                    log_fields["pipeline_avg_fps"] = round(queued_message.pipeline_avg_fps, 2)
                if queued_message.source_timestamp is not None:
                    log_fields["source_timestamp"] = queued_message.source_timestamp
                if ENABLE_TIMING_LOGS:
                    log_fields.update(
                        {
                            "queue_wait_ms": round((publish_started_at - queued_message.received_at) * 1000.0, 2),
                            "publish_wait_ms": round((publish_completed_at - publish_started_at) * 1000.0, 2),
                            "throttle_sleep_ms": round(throttle_sleep_ms, 2),
                            "bridge_total_ms": round((publish_completed_at - queued_message.received_at) * 1000.0, 2),
                            "publish_attempts": publish_attempts,
                            "drained_messages": drained,
                        }
                    )
                    if queued_message.source_time_ns is not None:
                        log_fields.update(
                            {
                                "pipeline_to_bridge_ms": round((publish_started_epoch_ns - queued_message.source_time_ns) / 1_000_000.0, 2),
                                "pipeline_to_thingsboard_ms": round((publish_completed_epoch_ns - queued_message.source_time_ns) / 1_000_000.0, 2),
                            }
                        )
                log(logging.INFO, "message_forwarded", **log_fields)
                break

            log(
                logging.WARNING,
                "publish_retry",
                route=route.name,
                topic=route.thingsboard_topic,
                rc=info.rc,
                queue_size=route.publish_queue.qsize(),
                retry_backoff_seconds=RETRY_BACKOFF_SECONDS,
            )
            time.sleep(RETRY_BACKOFF_SECONDS)

        route.publish_queue.task_done()


def main() -> int:
    global route_configs

    signal.signal(signal.SIGTERM, handle_signal)
    signal.signal(signal.SIGINT, handle_signal)

    route_configs = load_route_configs()
    log(
        logging.INFO,
        "bridge_starting",
        local_host=LOCAL_MQTT_HOST,
        local_port=LOCAL_MQTT_PORT,
        route_count=len(route_configs),
        local_topics=[route.local_topic for route in route_configs],
        thingsboard_host=THINGSBOARD_HOST,
        thingsboard_port=THINGSBOARD_PORT,
        qos=MQTT_QOS,
        min_publish_interval_seconds=MIN_PUBLISH_INTERVAL_SECONDS,
        enable_timing_logs=ENABLE_TIMING_LOGS,
        routes_file=THINGSBOARD_ROUTES_FILE or None,
    )

    local_client = configure_client(
        client_id="local-bridge",
        on_connect=on_local_connect,
        on_disconnect=on_local_disconnect,
        on_message=on_local_message,
    )

    for route in route_configs:
        route.tb_client = configure_client(
            client_id=f"tb-bridge-{route.name}",
            username=route.thingsboard_token,
            on_connect=on_tb_connect,
            on_disconnect=on_tb_disconnect,
            userdata={
                "route_name": route.name,
                "local_topic": route.local_topic,
                "thingsboard_topic": route.thingsboard_topic,
            },
        )
        route.tb_client.connect(THINGSBOARD_HOST, THINGSBOARD_PORT, keepalive=60)
        route.worker = threading.Thread(target=publish_worker, args=(route,), daemon=True)
        route.worker.start()

    local_client.connect(LOCAL_MQTT_HOST, LOCAL_MQTT_PORT, keepalive=60)

    local_client.loop_start()
    for route in route_configs:
        if route.tb_client is not None:
            route.tb_client.loop_start()

    try:
        while not stop_event.is_set():
            time.sleep(1)
    finally:
        stop_event.set()
        local_client.loop_stop()
        local_client.disconnect()
        for route in route_configs:
            if route.tb_client is not None:
                route.tb_client.loop_stop()
                route.tb_client.disconnect()
            if route.worker is not None:
                route.worker.join(timeout=5)
        log(logging.INFO, "bridge_stopped")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
