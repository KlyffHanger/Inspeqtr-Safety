"""Shared utility helpers for data coercion and payload transformation."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any


def coerce_bool(value: Any, default: bool = False) -> bool:
    """Convert common scalar input values into a boolean."""
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
    """Load JSON content from a file path."""
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def slugify(value: str) -> str:
    """Convert a human-readable string into a stable identifier fragment."""
    lowered = value.strip().lower()
    sanitized = "".join(char if char.isalnum() else "_" for char in lowered)
    while "__" in sanitized:
        sanitized = sanitized.replace("__", "_")
    return sanitized.strip("_") or "device"


def trim_string(value: str, limit: int) -> str:
    """Trim a string to a maximum length for telemetry transport."""
    if len(value) <= limit:
        return value
    return value[: max(0, limit - 3)] + "..."


def get_first(attrs: dict[str, Any], keys: list[str], default: Any = None) -> Any:
    """Return the first non-empty value found among a list of attribute keys."""
    for key in keys:
        if key in attrs and attrs[key] not in ("", None):
            return attrs[key]
    return default


def maybe_json_dict(value: Any) -> dict[str, Any]:
    """Parse a string as JSON only when it resolves to an object."""
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
    """Best-effort float conversion for external values."""
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str) and value.strip():
        try:
            return float(value)
        except ValueError:
            return None
    return None


def safe_int(value: Any) -> int | None:
    """Best-effort integer conversion for external values."""
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
