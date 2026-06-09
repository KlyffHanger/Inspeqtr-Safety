"""Structured logging utilities for the orchestration service."""

from __future__ import annotations

import json
import logging
from typing import Any


class JsonFormatter(logging.Formatter):
    """Serialize log records as JSON for machine-friendly ingestion."""

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


def configure_logging(log_level: str, logger_name: str) -> logging.Logger:
    """Configure root logging and return the application logger."""
    logging.basicConfig(level=log_level, format="%(message)s")
    logger = logging.getLogger(logger_name)
    for handler in logging.getLogger().handlers:
        handler.setFormatter(JsonFormatter())
    return logger


def structured_log(logger: logging.Logger, level: int, message: str, **extra: Any) -> None:
    """Write a structured application log entry."""
    logger.log(level, message, extra={"extra_fields": extra})
