"""DLStreamer pipeline lifecycle client."""

from __future__ import annotations

from typing import Any
from urllib.parse import quote

import requests

from klyff_bridge.config.settings import AppSettings


class PipelineController:
    """Client for DLStreamer pipeline lifecycle operations."""

    def __init__(self, settings: AppSettings) -> None:
        self.settings = settings
        self.session = requests.Session()

    def _api_path(self, path: str) -> str:
        normalized = "/" + path.lstrip("/")
        if self.settings.pipeline_server_api_prefix == "/":
            return normalized
        return f"{self.settings.pipeline_server_api_prefix}{normalized}"

    def _request(self, method: str, path: str, **kwargs: Any) -> requests.Response:
        response = self.session.request(
            method,
            f"{self.settings.pipeline_server_url}{self._api_path(path)}",
            timeout=self.settings.pipeline_request_timeout_seconds,
            verify=self.settings.pipeline_server_verify_tls,
            **kwargs,
        )
        response.raise_for_status()
        return response

    def status(self) -> list[dict[str, Any]]:
        """Return the pipeline status payload from DLStreamer."""
        response = self._request("GET", "/pipelines/status")
        payload = response.json()
        return payload if isinstance(payload, list) else []

    def running_instance_ids(self) -> set[str]:
        """Return the set of currently running pipeline instance IDs."""
        running = set()
        for item in self.status():
            if item.get("state") == "RUNNING" and isinstance(item.get("id"), str):
                running.add(item["id"])
        return running

    def start_pipeline(self, pipeline_name: str, payload: dict[str, Any]) -> str:
        """Start a pipeline instance and return the instance ID."""
        response = self._request(
            "POST",
            f"/pipelines/{quote(self.settings.pipeline_root, safe='')}/{quote(pipeline_name, safe='')}",
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
        """Stop a DLStreamer pipeline instance."""
        self._request("DELETE", f"/pipelines/{quote(instance_id, safe='')}")
