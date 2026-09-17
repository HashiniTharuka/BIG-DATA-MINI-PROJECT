"""Shared structured (JSON) logging setup used across every pipeline stage
so logs are consistently machine-parseable (stage, event, level, timestamp)
regardless of which component emits them.
"""
from __future__ import annotations

import json
import logging
import sys
import time


class JsonFormatter(logging.Formatter):
    def __init__(self, stage: str):
        super().__init__()
        self.stage = stage

    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(record.created)),
            "stage": self.stage,
            "level": record.levelname,
            "message": record.getMessage(),
        }
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        extra = getattr(record, "extra_fields", None)
        if extra:
            payload.update(extra)
        return json.dumps(payload)


def get_logger(stage: str) -> logging.Logger:
    """stage: short component name, e.g. 'vitals_producer', 'spark_streaming'."""
    logger = logging.getLogger(stage)
    if logger.handlers:
        return logger  # already configured (avoid duplicate handlers on reload)
    logger.setLevel(logging.INFO)
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter(stage))
    logger.addHandler(handler)
    logger.propagate = False
    return logger


def log_event(logger: logging.Logger, level: int, message: str, **fields) -> None:
    logger.log(level, message, extra={"extra_fields": fields})
