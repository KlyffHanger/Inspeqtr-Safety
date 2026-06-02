"""Application settings backed by environment variables."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class AppSettings(BaseSettings):
    """Runtime settings for the NVR orchestration service."""

    model_config = SettingsConfigDict(
        extra="ignore",
        case_sensitive=True,
    )

    log_level: str = Field(default="INFO", alias="LOG_LEVEL")

    pipeline_root: str = Field(default="user_defined_pipelines", alias="PIPELINE_ROOT")
    pipeline_server_url: str = Field(
        default="http://dlstreamer-pipeline-server:8080",
        alias="PIPELINE_SERVER_URL",
    )
    pipeline_server_api_prefix: str = Field(default="", alias="PIPELINE_SERVER_API_PREFIX")
    pipeline_server_verify_tls: bool = Field(default=False, alias="PIPELINE_SERVER_VERIFY_TLS")
    pipeline_request_timeout_seconds: float = Field(
        default=15.0,
        alias="PIPELINE_REQUEST_TIMEOUT_SECONDS",
    )

    local_mqtt_host: str = Field(default="mqtt-broker", alias="LOCAL_MQTT_HOST")
    local_mqtt_port: int = Field(default=1883, alias="LOCAL_MQTT_PORT")
    local_mqtt_qos: int = Field(default=0, alias="LOCAL_MQTT_QOS")

    klyff_api_base_url: str = Field(alias="KLYFF_API_BASE_URL")
    klyff_api_key: str = Field(default="", alias="KLYFF_API_KEY")
    klyff_username: str = Field(default="", alias="KLYFF_USERNAME")
    klyff_password: str = Field(default="", alias="KLYFF_PASSWORD")
    klyff_device_page_size: int = Field(default=100, alias="KLYFF_DEVICE_PAGE_SIZE")
    klyff_device_type_filter_raw: str = Field(default="", alias="KLYFF_DEVICE_TYPE_FILTER")
    klyff_require_nvr_enabled: bool = Field(default=True, alias="KLYFF_REQUIRE_NVR_ENABLED")
    klyff_sync_poll_seconds: float = Field(default=15.0, alias="KLYFF_SYNC_POLL_SECONDS")
    klyff_timeseries_key_prefix: str = Field(default="nvr_", alias="KLYFF_TIMESERIES_KEY_PREFIX")
    klyff_server_attr_scope: str = Field(default="SERVER_SCOPE", alias="KLYFF_SERVER_ATTR_SCOPE")
    klyff_telemetry_max_len: int = Field(default=4096, alias="KLYFF_TELEMETRY_MAX_LEN")

    nvr_base_config_file: Path = Field(
        default=Path("/config/nvr-base-config.json"),
        alias="NVR_BASE_CONFIG_FILE",
    )
    detection_config_file: Path = Field(
        default=Path("/config/detection-config.json"),
        alias="DETECTION_CONFIG_FILE",
    )
    pipeline_state_file: Path = Field(
        default=Path("/runtime/klyff-device-sync-state.json"),
        alias="PIPELINE_STATE_FILE",
    )

    device_source_uri_keys_raw: str = Field(
        default="sourceUri,source_uri,rtspUrl,rtsp_url",
        alias="NVR_SOURCE_URI_KEYS",
    )
    device_metadata_topic_keys_raw: str = Field(
        default="metadataTopic,metadata_topic",
        alias="NVR_METADATA_TOPIC_KEYS",
    )
    device_peer_id_keys_raw: str = Field(
        default="peerId,peer_id",
        alias="NVR_PEER_ID_KEYS",
    )
    device_pipeline_keys_raw: str = Field(
        default="pipeline,pipelineName",
        alias="NVR_PIPELINE_KEYS",
    )

    default_metadata_topic_prefix: str = Field(
        default="worker_safety_predictions_",
        alias="NVR_METADATA_TOPIC_PREFIX",
    )
    default_peer_id_prefix: str = Field(
        default="worker_safety_rtsp_",
        alias="NVR_PEER_ID_PREFIX",
    )
    default_source_type: str = Field(default="uri", alias="NVR_SOURCE_TYPE")
    default_validate_streams: bool = Field(default=True, alias="NVR_VALIDATE_STREAMS")
    stream_connect_timeout_seconds: float = Field(
        default=3.0,
        alias="NVR_STREAM_CONNECT_TIMEOUT_SECONDS",
    )

    mqtt_client_id: str = "klyff-dynamic-sync"
    logger_name: str = "klyff_dynamic_sync"

    @property
    def klyff_device_type_filter(self) -> set[str]:
        """Return the configured device-type filter as a normalized set."""
        return self._parse_csv_set(self.klyff_device_type_filter_raw)

    @property
    def device_source_uri_keys(self) -> list[str]:
        """Return candidate attribute keys for device source URIs."""
        return self._parse_csv_list(self.device_source_uri_keys_raw)

    @property
    def device_metadata_topic_keys(self) -> list[str]:
        """Return candidate attribute keys for metadata topics."""
        return self._parse_csv_list(self.device_metadata_topic_keys_raw)

    @property
    def device_peer_id_keys(self) -> list[str]:
        """Return candidate attribute keys for peer IDs."""
        return self._parse_csv_list(self.device_peer_id_keys_raw)

    @property
    def device_pipeline_keys(self) -> list[str]:
        """Return candidate attribute keys for pipeline names."""
        return self._parse_csv_list(self.device_pipeline_keys_raw)

    @field_validator("pipeline_server_url", "klyff_api_base_url", mode="before")
    @classmethod
    def _strip_trailing_slash(cls, value: Any) -> Any:
        """Normalize URL settings by removing trailing slashes."""
        if isinstance(value, str):
            return value.rstrip("/")
        return value

    @field_validator("pipeline_server_api_prefix", mode="before")
    @classmethod
    def _normalize_api_prefix(cls, value: Any) -> str:
        """Normalize the pipeline API prefix into a leading-slash path."""
        if value in (None, ""):
            return ""
        if not isinstance(value, str):
            raise TypeError("PIPELINE_SERVER_API_PREFIX must be a string")
        return "/" + value.strip().strip("/")

    @classmethod
    def _parse_csv_list(cls, value: Any) -> list[str]:
        """Convert comma-separated environment strings into lists."""
        if value in (None, ""):
            return []
        if isinstance(value, str):
            return [item.strip() for item in value.split(",") if item.strip()]
        if isinstance(value, list):
            return [str(item).strip() for item in value if str(item).strip()]
        raise TypeError("Expected a comma-separated string or list")

    @classmethod
    def _parse_csv_set(cls, value: Any) -> set[str]:
        """Convert comma-separated environment strings into sets."""
        if value in (None, ""):
            return set()
        if isinstance(value, str):
            return {item.strip() for item in value.split(",") if item.strip()}
        if isinstance(value, (list, set, tuple)):
            return {str(item).strip() for item in value if str(item).strip()}
        raise TypeError("Expected a comma-separated string or collection")
