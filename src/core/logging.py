"""
Structured logging configuration using structlog.
Outputs JSON in production, colored console in development.
"""
from __future__ import annotations

import logging
import sys

import structlog
from structlog.types import EventDict, WrappedLogger

from src.core.config import get_settings


def add_service_context(
    logger: WrappedLogger, method: str, event_dict: EventDict
) -> EventDict:
    settings = get_settings()
    event_dict["service"] = "person-network-analysis"
    event_dict["environment"] = settings.environment
    return event_dict


def configure_logging() -> None:
    settings = get_settings()
    log_level = getattr(logging, settings.log_level, logging.INFO)

    shared_processors: list = [
        structlog.contextvars.merge_contextvars,
        structlog.stdlib.add_log_level,
        structlog.processors.TimeStamper(fmt="iso"),
        add_service_context,
    ]

    if settings.environment == "development":
        renderer = structlog.dev.ConsoleRenderer(colors=True)
    else:
        renderer = structlog.processors.JSONRenderer()

    structlog.configure(
        processors=shared_processors
        + [
            structlog.stdlib.PositionalArgumentsFormatter(),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            renderer,
        ],
        wrapper_class=structlog.make_filtering_bound_logger(log_level),
        context_class=dict,
        logger_factory=structlog.PrintLoggerFactory(sys.stdout),
        cache_logger_on_first_use=True,
    )


def get_logger(name: str) -> structlog.BoundLogger:
    return structlog.get_logger(name)
