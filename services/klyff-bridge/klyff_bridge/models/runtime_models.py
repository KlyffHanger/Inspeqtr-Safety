"""Domain models for NVR orchestration state."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict


class DeviceRuntimeConfig(BaseModel):
    """Resolved desired runtime configuration for one device pipeline."""

    model_config = ConfigDict(extra="forbid")

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


class ManagedState(BaseModel):
    """Persisted runtime state for one managed device."""

    model_config = ConfigDict(extra="ignore")

    instance_id: str | None = None
    config_hash: str | None = None
    metadata_topic: str | None = None
    pipeline: str | None = None
    peer_id: str | None = None
    source_uri: str | None = None
    last_status: str | None = None
    last_error: str | None = None
