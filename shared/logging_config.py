"""
Configure structlog to emit JSON logs in production, pretty-print in dev.
Call configure_logging() once at process startup.
"""
from __future__ import annotations

import logging
import os
import sys

import structlog


def configure_logging() -> None:
    env = os.environ.get("ENV", "development")
    level = logging.DEBUG if env == "development" else logging.INFO

    shared_processors = [
        structlog.contextvars.merge_contextvars,
        structlog.stdlib.add_logger_name,
        structlog.stdlib.add_log_level,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.StackInfoRenderer(),
    ]

    if env == "production":
        # JSON for log aggregators (Railway, Datadog, etc.)
        renderer = structlog.processors.JSONRenderer()
    else:
        renderer = structlog.dev.ConsoleRenderer(colors=True)

    structlog.configure(
        processors=[
            *shared_processors,
            structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
        ],
        wrapper_class=structlog.stdlib.BoundLogger,
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )

    formatter = structlog.stdlib.ProcessorFormatter(
        processor=renderer,
        foreign_pre_chain=shared_processors,
    )

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(formatter)

    root = logging.getLogger()
    root.handlers = [handler]
    root.setLevel(level)

    # Silence noisy libs
    for noisy in ("uvicorn.access", "kafka", "confluent_kafka"):
        logging.getLogger(noisy).setLevel(logging.WARNING)
