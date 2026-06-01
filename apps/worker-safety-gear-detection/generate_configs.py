#!/usr/bin/env python3
import json
from pathlib import Path


APP_DIR = Path(__file__).resolve().parent
CAMERAS_FILE = APP_DIR / "cameras.json"
DETECTION_CONFIG_FILE = APP_DIR / "detection-config.json"
PAYLOAD_OUTPUT_FILE = APP_DIR / "payload.json"
ROUTES_OUTPUT_FILE = APP_DIR / "thingsboard-routes.json"


def load_json(path: Path):
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def resolve_pipeline(camera: dict, topology: dict) -> str:
    if "pipeline" in camera:
        return camera["pipeline"]

    if topology.get("mode") == "shared_nvr":
        pipeline = topology.get("pipeline")
        if pipeline:
            return pipeline

    raise SystemExit(f"Camera entry is missing pipeline: {camera}")


def build_shared_detection_properties(topology: dict, shared_stream_count: int) -> dict:
    if topology.get("mode") != "shared_nvr":
        return {}

    batch_size = topology.get("batch_size", "auto")
    if batch_size == "auto":
        batch_size = shared_stream_count

    shared_properties = {
        "model-instance-id": topology.get("model_instance_id", "instnvr0"),
        "batch-size": batch_size,
        "nireq": topology.get("nireq", 2),
        "inference-interval": topology.get("inference_interval", 1),
    }

    threshold = topology.get("threshold")
    if threshold is not None:
        shared_properties["threshold"] = threshold

    return shared_properties


def build_payload(
    camera: dict,
    default_device: str,
    detection_config: dict,
    topology: dict,
    shared_detection_properties: dict,
) -> dict:
    device = camera.get("device", default_device)
    destination: dict[str, dict] = {}

    metadata_topic = camera.get("metadata_topic")
    if metadata_topic:
        destination["metadata"] = {
            "type": "mqtt",
            "publish_frame": bool(camera.get("publish_frame", False)),
            "topic": metadata_topic,
        }

    peer_id = camera.get("peer_id")
    if peer_id:
        destination["frame"] = {
            "type": "webrtc",
            "peer-id": peer_id,
        }

    detection_properties = {
        "device": device,
        **detection_config,
    }

    if camera.get("use_shared_pipeline", True):
        detection_properties.update(shared_detection_properties)

    detection_properties.update(topology.get("detection_properties", {}))
    detection_properties.update(camera.get("detection_properties", {}))

    return {
        "pipeline": resolve_pipeline(camera, topology),
        "payload": {
            "source": {
                "uri": camera["source_uri"],
                "type": camera.get("source_type", "uri"),
            },
            "destination": destination,
            "parameters": {
                "detection-properties": detection_properties
            },
        },
    }


def build_route(camera: dict) -> dict | None:
    metadata_topic = camera.get("metadata_topic")
    if not metadata_topic:
        return None

    route = {
        "name": camera["id"],
        "local_topic": metadata_topic,
    }

    token = camera.get("thingsboard_token")
    if token:
        route["thingsboard_token"] = token

    token_env = camera.get("thingsboard_token_env")
    if token_env:
        route["thingsboard_token_env"] = token_env

    thingsboard_topic = camera.get("thingsboard_topic")
    if thingsboard_topic:
        route["thingsboard_topic"] = thingsboard_topic

    return route


def main() -> int:
    inventory = load_json(CAMERAS_FILE)
    detection_config = load_json(DETECTION_CONFIG_FILE)

    cameras = [camera for camera in inventory.get("cameras", []) if camera.get("enabled", True)]
    if not isinstance(cameras, list) or not cameras:
        raise SystemExit("cameras.json must contain a non-empty cameras array")

    default_device = inventory.get("default_device", "CPU")
    topology = inventory.get("topology", {})
    shared_stream_count = len([camera for camera in cameras if camera.get("use_shared_pipeline", True)])
    shared_detection_properties = build_shared_detection_properties(topology, shared_stream_count)

    payloads = []
    routes = []
    for camera in cameras:
        payloads.append(
            build_payload(
                camera,
                default_device,
                detection_config,
                topology,
                shared_detection_properties,
            )
        )
        route = build_route(camera)
        if route is not None:
            routes.append(route)

    PAYLOAD_OUTPUT_FILE.write_text(json.dumps(payloads, indent=2) + "\n", encoding="utf-8")
    ROUTES_OUTPUT_FILE.write_text(json.dumps(routes, indent=2) + "\n", encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
