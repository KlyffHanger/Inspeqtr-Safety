import base64
import binascii
import json
import os
import re
import threading
import time
import urllib.error
import urllib.parse
import urllib.request

from thingsboard_gateway.connectors.mqtt.mqtt_uplink_converter import MqttUplinkConverter
from thingsboard_gateway.gateway.entities.converted_data import ConvertedData
from thingsboard_gateway.gateway.entities.telemetry_entry import TelemetryEntry


class CustomMqttUplinkConverter(MqttUplinkConverter):
    ALARM_TYPE_BY_LABEL = {
        "NO-Hardhat": "PPE Missing Hardhat",
        "NO-Mask": "PPE Missing Mask",
        "NO-Safety Vest": "PPE Missing Safety Vest",
    }

    ALARM_SEVERITY_BY_LABEL = {
        "NO-Hardhat": "CRITICAL",
        "NO-Mask": "MAJOR",
        "NO-Safety Vest": "MAJOR",
    }

    def __init__(self, config, logger):
        self._log = logger
        self._config = config.get("converter", config)
        self._state_by_device = {}
        self._alarm_state_by_device = {}
        self._device_id_cache = {}
        self._frame_cache = {}  # Cache frame references by (device_name, frame_id)
        self._http_lock = threading.Lock()
        self._tb_rest_base_url = os.getenv("TB_REST_BASE_URL", "http://thingsboard-ce:8080").rstrip("/")
        self._tb_rest_username = os.getenv("TB_REST_USERNAME", "tenant@thingsboard.org")
        self._tb_rest_password = os.getenv("TB_REST_PASSWORD", "tenant")
        self._tb_jwt = None
        self._tb_jwt_expiry = 0

    def convert(self, topic, body):
        try:
            payload = self._decode_payload(body)
            extension_config = self._config.get("extensionConfig") or self._config.get("extension-config") or {}
            device_name = extension_config.get("deviceName") or self._config.get("deviceInfo", {}).get("deviceNameExpression") or topic.split("/")[-1]
            device_profile = extension_config.get("deviceProfile") or self._config.get("deviceInfo", {}).get("deviceProfileExpression") or "default"
            monitored_labels = extension_config.get("labels") or []
            alert_interval_ms = int(extension_config.get("alertIntervalMs", 10000))
            send_clear_event = bool(extension_config.get("sendClearEvent", True))
            frames_directory = extension_config.get("framesDirectory", "/thingsboard_gateway/frames")
            frames_reference_prefix = extension_config.get("framesReferencePrefix", "alarm-frames")
            dashboard_base_url = (extension_config.get("dashboardBaseUrl") or "").rstrip("/")
            live_stream_path = extension_config.get("liveStreamPath") or "/mediamtx/worker_safety/"
            camera_label = extension_config.get("cameraLabel") or device_name

            metadata = payload.get("metadata") or {}
            labels = self._extract_labels(metadata)
            monitored_hits = sorted(label for label in labels if label in monitored_labels)
            unique_labels = sorted(set(labels))
            timestamp_ms = self._get_timestamp_ms(metadata)
            frame_id = metadata.get("frame_id")
            frame_reference = None
            
            # Save frame once per unique frame_id, cache the reference
            if monitored_hits:
                cache_key = (device_name, frame_id)
                if cache_key not in self._frame_cache:
                    frame_reference = self._save_frame_reference(
                        device_name=device_name,
                        timestamp_ms=timestamp_ms,
                        metadata=metadata,
                        blob=payload.get("blob"),
                        frames_directory=frames_directory,
                        frames_reference_prefix=frames_reference_prefix,
                    )
                    if frame_reference:
                        self._frame_cache[cache_key] = frame_reference
                        self._log.debug("Cached frame: device=%s, frame_id=%s, ref=%s", device_name, frame_id, frame_reference)
                else:
                    frame_reference = self._frame_cache[cache_key]
                    self._log.debug("Using cached frame: device=%s, frame_id=%s", device_name, frame_id)

            previous_state = self._state_by_device.get(device_name, {"labels": tuple(), "last_sent_ts": 0})
            current_state = tuple(monitored_hits)

            if not monitored_hits:
                self._state_by_device[device_name] = {"labels": tuple(), "last_sent_ts": previous_state.get("last_sent_ts", 0)}
                if send_clear_event and previous_state.get("labels"):
                    return self._build_message(
                        device_name=device_name,
                        device_profile=device_profile,
                        timestamp_ms=timestamp_ms,
                        topic=topic,
                        metadata=metadata,
                        monitored_labels=monitored_labels,
                        unique_labels=unique_labels,
                        monitored_hits=[],
                        event_type="cleared",
                        frame_reference=None,
                        frame_url=None,
                        live_stream_url=self._build_public_url(dashboard_base_url, live_stream_path),
                        camera_label=camera_label,
                    )
                return None

            if previous_state.get("labels") == current_state and timestamp_ms and timestamp_ms - previous_state.get("last_sent_ts", 0) < alert_interval_ms:
                return None

            self._state_by_device[device_name] = {"labels": current_state, "last_sent_ts": timestamp_ms or previous_state.get("last_sent_ts", 0)}
            frame_url = self._build_public_url(dashboard_base_url, frame_reference) if frame_reference else None
            return self._build_message(
                device_name=device_name,
                device_profile=device_profile,
                timestamp_ms=timestamp_ms,
                topic=topic,
                metadata=metadata,
                monitored_labels=monitored_labels,
                unique_labels=unique_labels,
                monitored_hits=monitored_hits,
                event_type="raised",
                frame_reference=frame_reference,
                frame_url=frame_url,
                live_stream_url=self._build_public_url(dashboard_base_url, live_stream_path),
                camera_label=camera_label,
            )
        except Exception:
            self._log.exception("Failed to convert MQTT message from topic %s", topic)
            return None

    def _build_message(self, device_name, device_profile, timestamp_ms, topic, metadata, monitored_labels, unique_labels, monitored_hits, event_type, frame_reference, frame_url, live_stream_url, camera_label):
        converted = ConvertedData(device_name=device_name, device_type=device_profile)
        label_counts = {label: monitored_hits.count(label) for label in monitored_labels}
        self._sync_alarms(device_name, timestamp_ms, metadata, label_counts, frame_reference, frame_url, live_stream_url, camera_label)

        telemetry = {
            "ppe_violation": bool(monitored_hits),
            "ppe_violation_event": event_type,
            "ppe_violation_count": len(monitored_hits),
            "ppe_violation_labels": json.dumps(monitored_hits),
            "detected_labels": json.dumps(unique_labels),
            "frame_id": metadata.get("frame_id"),
            "camera_name": camera_label,
            "live_stream_url": live_stream_url,
        }
        if frame_reference:
            telemetry["frame_reference"] = frame_reference
        if frame_url:
            telemetry["frame_url"] = frame_url
            telemetry["image_url"] = frame_url
            telemetry["snapshot_url"] = frame_url

        for label in monitored_labels:
            count = label_counts[label]
            telemetry[self._count_key(label)] = count
            telemetry[self._flag_key(label)] = count > 0
            telemetry[self._event_key(label)] = "raised" if count > 0 else "cleared"

        if timestamp_ms is None:
            converted.add_to_telemetry(telemetry)
        else:
            converted.add_to_telemetry(TelemetryEntry(telemetry, ts=timestamp_ms))

        converted.add_to_attributes(
            {
                "mqtt_topic": topic,
                "pipeline_name": (metadata.get("pipeline") or {}).get("name"),
                "pipeline_version": (metadata.get("pipeline") or {}).get("version"),
                "monitored_labels": json.dumps(monitored_labels),
                "camera_name": camera_label,
                "live_stream_url": live_stream_url,
            }
        )
        return converted

    def _decode_payload(self, body):
        if isinstance(body, dict):
            return body
        if isinstance(body, bytes):
            body = body.decode("utf-8")
        return json.loads(body)

    def _sync_alarms(self, device_name, timestamp_ms, metadata, label_counts, frame_reference, frame_url, live_stream_url, camera_label):
        active_alarm_ids = self._alarm_state_by_device.setdefault(device_name, {})
        device_id = self._get_device_id(device_name)
        if not device_id:
            return

        event_ts = timestamp_ms or int(time.time() * 1000)

        for label, count in label_counts.items():
            alarm_type = self.ALARM_TYPE_BY_LABEL.get(label)
            if not alarm_type:
                continue

            if count > 0:
                # Check if alarm already exists for this label
                alarm_id = active_alarm_ids.get(label)
                
                if alarm_id:
                    # Alarm exists - UPDATE it with new frame evidence (no duplicate alarms)
                    self._update_alarm_with_frame(
                        alarm_id,
                        device_id,
                        alarm_type,
                        label,
                        count,
                        event_ts,
                        metadata,
                        frame_reference,
                        frame_url,
                        live_stream_url,
                        camera_label,
                    )
                else:
                    # Alarm doesn't exist - CREATE new one
                    alarm_id = self._create_or_update_alarm(
                        device_id,
                        alarm_type,
                        label,
                        count,
                        event_ts,
                        metadata,
                        frame_reference,
                        frame_url,
                        live_stream_url,
                        camera_label,
                    )
                    if alarm_id:
                        active_alarm_ids[label] = alarm_id

    def _get_device_id(self, device_name):
        cached = self._device_id_cache.get(device_name)
        if cached:
            return cached

        encoded_name = urllib.parse.quote(device_name, safe="")
        response = self._tb_request("GET", f"/api/tenant/devices?deviceName={encoded_name}")
        if not response or not response.get("id"):
            self._log.warning("Unable to resolve ThingsBoard device id for %s", device_name)
            return None

        device_id = response["id"]["id"]
        self._device_id_cache[device_name] = device_id
        return device_id

    def _create_or_update_alarm(self, device_id, alarm_type, label, count, event_ts, metadata, frame_reference, frame_url, live_stream_url, camera_label):
        details = {
            "label": label,
            "count": count,
            "frame_id": metadata.get("frame_id"),
            "detected_labels": self._extract_labels(metadata),
            "camera_name": camera_label,
            "live_stream_url": live_stream_url,
        }
        if frame_reference:
            details["frame_reference"] = frame_reference
        if frame_url:
            details["frame_url"] = frame_url
            details["image_url"] = frame_url
            details["snapshot_url"] = frame_url

        payload = {
            "type": alarm_type,
            "originator": {
                "entityType": "DEVICE",
                "id": device_id,
            },
            "severity": self.ALARM_SEVERITY_BY_LABEL.get(label, "MAJOR"),
            "acknowledged": False,
            "cleared": False,
            "startTs": event_ts,
            "endTs": event_ts,
            "details": details,
            "propagate": False,
            "propagateToOwner": False,
            "propagateToTenant": False,
        }
        response = self._tb_request("POST", "/api/alarm", payload)
        if response and response.get("id"):
            return response["id"]["id"]
        return None

    def _clear_alarm(self, alarm_id):
        response = self._tb_request("POST", f"/api/alarm/{alarm_id}/clear")
        return bool(response and response.get("cleared") is True)

    def _update_alarm_with_frame(self, alarm_id, device_id, alarm_type, label, count, event_ts, metadata, frame_reference, frame_url, live_stream_url, camera_label):
        """
        Update an existing alarm by adding new frame evidence without creating duplicate alarms.
        Just updates the latest frame and timestamp, keeping the same alarm active.
        """
        details = {
            "label": label,
            "count": count,
            "frame_id": metadata.get("frame_id"),
            "detected_labels": self._extract_labels(metadata),
            "camera_name": camera_label,
            "live_stream_url": live_stream_url,
        }
        if frame_reference:
            details["frame_reference"] = frame_reference
        if frame_url:
            details["frame_url"] = frame_url
            details["image_url"] = frame_url
            details["snapshot_url"] = frame_url

        # Update the alarm's endTs to reflect latest activity
        # Keep it ACTIVE, don't create new alarm
        payload = {
            "endTs": event_ts,
            "details": details,
        }
        
        response = self._tb_request("PUT", f"/api/alarm/{alarm_id}", payload)
        if response:
            self._log.debug("Updated alarm %s with new frame evidence: label=%s, frame_id=%s", alarm_id, label, metadata.get("frame_id"))
        return alarm_id

    def _save_frame_reference(self, device_name, timestamp_ms, metadata, blob, frames_directory, frames_reference_prefix):
        image_bytes = self._decode_blob(blob)
        if not image_bytes:
            return None

        frame_ts = timestamp_ms or int(time.time() * 1000)
        safe_device_name = re.sub(r"[^A-Za-z0-9._-]+", "-", device_name).strip("-") or "device"
        frame_id = metadata.get("frame_id")
        
        # Build unique filename based on frame_id (primary) or timestamp + uuid (fallback)
        if frame_id is not None:
            filename = f"frame-{frame_id}.jpg"
        else:
            import uuid
            unique_suffix = str(uuid.uuid4())[:8]
            filename = f"{frame_ts}_{unique_suffix}.jpg"

        device_directory = os.path.join(frames_directory, safe_device_name)
        os.makedirs(device_directory, exist_ok=True)

        file_path = os.path.join(device_directory, filename)
        # Only write if file doesn't exist (avoid overwriting)
        if not os.path.exists(file_path):
            with open(file_path, "wb") as frame_file:
                frame_file.write(image_bytes)
        
        # Keep cache size manageable (last 1000 frames per device)
        try:
            cache_key_prefix = device_name
            cache_keys = [k for k in self._frame_cache.keys() if k and len(k) > 0 and k[0] == cache_key_prefix]
            if len(cache_keys) > 1000:
                # Remove oldest entries (keep newest 1000)
                for old_key in sorted(cache_keys)[:-1000]:
                    if old_key in self._frame_cache:
                        del self._frame_cache[old_key]
        except Exception as e:
            self._log.warning("Error managing frame cache: %s", str(e))

        return "/".join(
            [
                frames_reference_prefix.rstrip("/"),
                safe_device_name,
                filename,
            ]
        )

    def _decode_blob(self, blob):
        if blob in (None, ""):
            return None

        if isinstance(blob, bytes):
            return blob

        if not isinstance(blob, str):
            return None

        blob = blob.strip()
        if not blob:
            return None

        if blob.startswith("data:"):
            _, _, blob = blob.partition(",")

        try:
            return base64.b64decode(blob, validate=True)
        except (binascii.Error, ValueError):
            self._log.warning("Unable to decode MQTT frame blob as base64 JPEG")
            return None

    def _tb_request(self, method, path, payload=None, retry=True):
        with self._http_lock:
            token = self._get_jwt()
            if not token:
                return None

            body = None if payload is None else json.dumps(payload).encode("utf-8")
            request = urllib.request.Request(
                f"{self._tb_rest_base_url}{path}",
                data=body,
                method=method,
                headers={
                    "Content-Type": "application/json",
                    "X-Authorization": f"Bearer {token}",
                },
            )

            try:
                with urllib.request.urlopen(request, timeout=5) as response:
                    response_body = response.read().decode("utf-8")
                    return json.loads(response_body) if response_body else {}
            except urllib.error.HTTPError as exc:
                if exc.code == 401 and retry:
                    self._tb_jwt = None
                    self._tb_jwt_expiry = 0
                    return self._tb_request(method, path, payload, retry=False)
                self._log.warning("ThingsBoard REST request failed: %s %s returned %s", method, path, exc.code)
            except Exception:
                self._log.exception("ThingsBoard REST request failed: %s %s", method, path)
            return None

    def _get_jwt(self):
        if self._tb_jwt and time.time() < self._tb_jwt_expiry:
            return self._tb_jwt

        login_payload = json.dumps(
            {
                "username": self._tb_rest_username,
                "password": self._tb_rest_password,
            }
        ).encode("utf-8")
        request = urllib.request.Request(
            f"{self._tb_rest_base_url}/api/auth/login",
            data=login_payload,
            method="POST",
            headers={"Content-Type": "application/json"},
        )

        try:
            with urllib.request.urlopen(request, timeout=5) as response:
                data = json.loads(response.read().decode("utf-8"))
                self._tb_jwt = data.get("token")
                self._tb_jwt_expiry = time.time() + 600
                return self._tb_jwt
        except Exception:
            self._log.exception("Failed to authenticate to ThingsBoard REST API")
            self._tb_jwt = None
            self._tb_jwt_expiry = 0
            return None

    def _extract_labels(self, metadata):
        labels = []
        for obj in metadata.get("gva_meta") or []:
            for tensor in obj.get("tensor") or []:
                label = tensor.get("label")
                if label:
                    labels.append(label)

        if labels:
            return labels

        for obj in metadata.get("objects") or []:
            detection = obj.get("detection") or {}
            label = detection.get("label")
            if label:
                labels.append(label)
        return labels

    def _get_timestamp_ms(self, metadata):
        event_time_ns = metadata.get("time")
        if isinstance(event_time_ns, (int, float)):
            return int(event_time_ns / 1_000_000)
        return None

    def _count_key(self, label):
        slug = re.sub(r"[^a-z0-9]+", "_", label.lower()).strip("_")
        return f"{slug}_count"

    def _flag_key(self, label):
        slug = re.sub(r"[^a-z0-9]+", "_", label.lower()).strip("_")
        return f"{slug}_violation"

    def _event_key(self, label):
        slug = re.sub(r"[^a-z0-9]+", "_", label.lower()).strip("_")
        return f"{slug}_event"

    def _build_public_url(self, base_url, path):
        if not path:
            return None
        if path.startswith("http://") or path.startswith("https://"):
            return path
        normalized_path = path if path.startswith("/") else f"/{path}"
        return f"{base_url}{normalized_path}" if base_url else normalized_path
