"""KLYFF REST client."""

from __future__ import annotations

from typing import Any
from urllib.parse import quote

import requests

from klyff_bridge.config.settings import AppSettings


class KlyffRestClient:
    """Client for KLYFF device, attribute, and telemetry APIs."""

    def __init__(self, settings: AppSettings) -> None:
        self.settings = settings
        self.session = requests.Session()
        self._auth_header: str | None = None

    def _ensure_auth(self) -> str:
        if self.settings.klyff_api_key:
            return f"ApiKey {self.settings.klyff_api_key}"

        if self._auth_header:
            return self._auth_header

        if not self.settings.klyff_username or not self.settings.klyff_password:
            raise RuntimeError("Set KLYFF_API_KEY or KLYFF_USERNAME/KLYFF_PASSWORD")

        response = self.session.post(
            f"{self.settings.klyff_api_base_url}/api/auth/login",
            json={
                "username": self.settings.klyff_username,
                "password": self.settings.klyff_password,
            },
            timeout=self.settings.pipeline_request_timeout_seconds,
        )
        response.raise_for_status()
        payload = response.json()
        token = payload.get("token")
        if not isinstance(token, str) or not token:
            raise RuntimeError("KLYFF login did not return a token")
        self._auth_header = f"Bearer {token}"
        return self._auth_header

    def _request(self, method: str, path: str, **kwargs: Any) -> requests.Response:
        headers = kwargs.pop("headers", {})
        headers["X-Authorization"] = self._ensure_auth()
        response = self.session.request(
            method,
            f"{self.settings.klyff_api_base_url}{path}",
            headers=headers,
            timeout=self.settings.pipeline_request_timeout_seconds,
            **kwargs,
        )
        if response.status_code == 401 and not self.settings.klyff_api_key:
            self._auth_header = None
            headers["X-Authorization"] = self._ensure_auth()
            response = self.session.request(
                method,
                f"{self.settings.klyff_api_base_url}{path}",
                headers=headers,
                timeout=self.settings.pipeline_request_timeout_seconds,
                **kwargs,
            )
        response.raise_for_status()
        return response

    def list_devices(self) -> list[dict[str, Any]]:
        """Fetch all tenant devices across paginated responses."""
        devices: list[dict[str, Any]] = []
        page = 0
        while True:
            response = self._request(
                "GET",
                f"/api/tenant/devices?pageSize={self.settings.klyff_device_page_size}&page={page}",
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
        """Fetch all attributes for a device."""
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
        """Persist server-scope attributes for a device."""
        self._request(
            "POST",
            f"/api/plugins/telemetry/DEVICE/{quote(device_id, safe='')}/attributes/{quote(self.settings.klyff_server_attr_scope, safe='')}",
            json=attrs,
            headers={"Content-Type": "application/json"},
        )

    def save_timeseries(self, device_id: str, values: dict[str, Any], ts_ms: int | None = None) -> None:
        """Persist timeseries telemetry for a device."""
        payload: dict[str, Any]
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
