import json
import logging
import os
import queue
import signal
import threading
import time
from typing import Any

import paho.mqtt.client as mqtt


def env(name: str, default: str | None = None, required: bool = False) -> str:
    value = os.getenv(name, default)
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
LOCAL_MQTT_TOPIC = env("LOCAL_MQTT_TOPIC", required=True)
THINGSBOARD_HOST = env("THINGSBOARD_HOST", required=True)
THINGSBOARD_PORT = env_int("THINGSBOARD_PORT", 1883)
THINGSBOARD_TOKEN = env("THINGSBOARD_TOKEN", required=True)
THINGSBOARD_TOPIC = env("THINGSBOARD_TOPIC", "v1/devices/me/telemetry")
MQTT_QOS = env_int("MQTT_QOS", 0)
QUEUE_MAXSIZE = env_int("QUEUE_MAXSIZE", 1)
RETRY_BACKOFF_SECONDS = env_float("RETRY_BACKOFF_SECONDS", 2.0)
PUBLISH_TIMEOUT_SECONDS = env_float("PUBLISH_TIMEOUT_SECONDS", 10.0)
MIN_PUBLISH_INTERVAL_SECONDS = env_float("MIN_PUBLISH_INTERVAL_SECONDS", 2.0)

stop_event = threading.Event()
publish_queue: queue.Queue[str] = queue.Queue(maxsize=QUEUE_MAXSIZE)


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
) -> mqtt.Client:
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=client_id)
    if username:
        client.username_pw_set(username)
    if on_connect:
        client.on_connect = on_connect
    if on_disconnect:
        client.on_disconnect = on_disconnect
    if on_message:
        client.on_message = on_message
    client.reconnect_delay_set(min_delay=1, max_delay=30)
    return client


def on_local_connect(client: mqtt.Client, _userdata: Any, _flags: Any, reason_code: Any, _properties: Any) -> None:
    if reason_code != 0:
        log(logging.ERROR, "local_broker_connect_failed", host=LOCAL_MQTT_HOST, port=LOCAL_MQTT_PORT, reason=str(reason_code))
        return
    client.subscribe(LOCAL_MQTT_TOPIC, qos=MQTT_QOS)
    log(logging.INFO, "local_broker_connected", host=LOCAL_MQTT_HOST, port=LOCAL_MQTT_PORT, topic=LOCAL_MQTT_TOPIC, qos=MQTT_QOS)


def on_local_disconnect(_client: mqtt.Client, _userdata: Any, _flags: Any, reason_code: Any, _properties: Any) -> None:
    log(logging.WARNING, "local_broker_disconnected", reason=str(reason_code))


def on_local_message(_client: mqtt.Client, _userdata: Any, msg: mqtt.MQTTMessage) -> None:
    payload = msg.payload.decode("utf-8", errors="replace")
    try:
        publish_queue.put(payload, timeout=1)
        log(logging.INFO, "message_queued", topic=msg.topic, queue_size=publish_queue.qsize())
    except queue.Full:
        try:
            publish_queue.get_nowait()
            publish_queue.task_done()
        except queue.Empty:
            pass

        try:
            publish_queue.put_nowait(payload)
            log(logging.WARNING, "message_replaced_queue_full", topic=msg.topic, queue_size=publish_queue.qsize())
        except queue.Full:
            log(logging.ERROR, "message_dropped_queue_full", topic=msg.topic, queue_size=publish_queue.qsize())


def on_tb_connect(_client: mqtt.Client, _userdata: Any, _flags: Any, reason_code: Any, _properties: Any) -> None:
    if reason_code != 0:
        log(logging.ERROR, "thingsboard_connect_failed", host=THINGSBOARD_HOST, port=THINGSBOARD_PORT, reason=str(reason_code))
        return
    log(logging.INFO, "thingsboard_connected", host=THINGSBOARD_HOST, port=THINGSBOARD_PORT, topic=THINGSBOARD_TOPIC, qos=MQTT_QOS)


def on_tb_disconnect(_client: mqtt.Client, _userdata: Any, _flags: Any, reason_code: Any, _properties: Any) -> None:
    log(logging.WARNING, "thingsboard_disconnected", reason=str(reason_code))


def publish_worker(tb_client: mqtt.Client) -> None:
    last_publish_time = 0.0
    while not stop_event.is_set():
        try:
            payload = publish_queue.get(timeout=1)
        except queue.Empty:
            continue

        # Keep only the newest queued message before publishing. This prevents
        # the bridge from lagging behind when frame metadata arrives faster than
        # ThingsBoard can ingest it.
        drained = 0
        while True:
            try:
                payload = publish_queue.get_nowait()
                publish_queue.task_done()
                drained += 1
            except queue.Empty:
                break

        if drained:
            log(logging.INFO, "queue_drained_to_latest", dropped_messages=drained, queue_size=publish_queue.qsize())

        remaining_interval = MIN_PUBLISH_INTERVAL_SECONDS - (time.time() - last_publish_time)
        if remaining_interval > 0:
            time.sleep(remaining_interval)

        while not stop_event.is_set():
            info = tb_client.publish(THINGSBOARD_TOPIC, payload, qos=MQTT_QOS)
            published = True if MQTT_QOS == 0 else info.wait_for_publish(timeout=PUBLISH_TIMEOUT_SECONDS)
            if published and info.rc == mqtt.MQTT_ERR_SUCCESS:
                last_publish_time = time.time()
                log(logging.INFO, "message_forwarded", topic=THINGSBOARD_TOPIC, queue_size=publish_queue.qsize())
                break

            log(
                logging.WARNING,
                "publish_retry",
                topic=THINGSBOARD_TOPIC,
                rc=info.rc,
                queue_size=publish_queue.qsize(),
                retry_backoff_seconds=RETRY_BACKOFF_SECONDS,
            )
            time.sleep(RETRY_BACKOFF_SECONDS)

        publish_queue.task_done()


def main() -> int:
    signal.signal(signal.SIGTERM, handle_signal)
    signal.signal(signal.SIGINT, handle_signal)

    log(
        logging.INFO,
        "bridge_starting",
        local_host=LOCAL_MQTT_HOST,
        local_port=LOCAL_MQTT_PORT,
        local_topic=LOCAL_MQTT_TOPIC,
        thingsboard_host=THINGSBOARD_HOST,
        thingsboard_port=THINGSBOARD_PORT,
        thingsboard_topic=THINGSBOARD_TOPIC,
        qos=MQTT_QOS,
        min_publish_interval_seconds=MIN_PUBLISH_INTERVAL_SECONDS,
    )

    local_client = configure_client(
        client_id=f"local-bridge-{LOCAL_MQTT_TOPIC}",
        on_connect=on_local_connect,
        on_disconnect=on_local_disconnect,
        on_message=on_local_message,
    )
    tb_client = configure_client(
        client_id=f"tb-bridge-{LOCAL_MQTT_TOPIC}",
        username=THINGSBOARD_TOKEN,
        on_connect=on_tb_connect,
        on_disconnect=on_tb_disconnect,
    )

    local_client.connect(LOCAL_MQTT_HOST, LOCAL_MQTT_PORT, keepalive=60)
    tb_client.connect(THINGSBOARD_HOST, THINGSBOARD_PORT, keepalive=60)

    worker = threading.Thread(target=publish_worker, args=(tb_client,), daemon=True)
    worker.start()

    local_client.loop_start()
    tb_client.loop_start()

    try:
        while not stop_event.is_set():
            time.sleep(1)
    finally:
        stop_event.set()
        local_client.loop_stop()
        tb_client.loop_stop()
        local_client.disconnect()
        tb_client.disconnect()
        worker.join(timeout=5)
        log(logging.INFO, "bridge_stopped")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
