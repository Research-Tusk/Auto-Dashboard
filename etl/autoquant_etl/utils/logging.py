"""
AutoQuant ETL — Logging Configuration
=======================================
Configures structlog with a console renderer suitable for both
interactive CLI use and machine-readable CI/CD output.
"""

from __future__ import annotations

import logging
import sys

import structlog

from autoquant_etl.config import LogLevel


def configure_logging(level: LogLevel) -> None:
    """
    Configure structlog for the ETL process.

    Sets up:
    - stdlib logging integration
    - ISO timestamp processor
    - Log level filtering
    - Console renderer (colored if TTY, plain otherwise)

    Args:
        level: LogLevel enum value (DEBUG / INFO / WARNING / ERROR)
    """
    log_level = getattr(logging, level.value, logging.INFO)

    # Configure stdlib logging so asyncpg / playwright logs flow through
    logging.basicConfig(
        format="%(message)s",
        stream=sys.stdout,
        level=log_level,
    )

    # Shared processors for both stdlib and structlog
    shared_processors: list = [
        structlog.contextvars.merge_contextvars,
        structlog.stdlib.add_log_level,
        structlog.stdlib.add_logger_name,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.StackInfoRenderer(),
    ]

    structlog.configure(
        processors=shared_processors
        + [
            structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
        ],
        wrapper_class=structlog.stdlib.BoundLogger,
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )

    # Choose renderer: colored for interactive TTY, plain for CI
    if sys.stdout.isatty():
        renderer = structlog.dev.ConsoleRenderer(colors=True)
    else:
        renderer = structlog.processors.JSONRenderer()

    formatter = structlog.stdlib.ProcessorFormatter(
        processor=renderer,
        foreign_pre_chain=shared_processors,
    )

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(formatter)

    root_logger = logging.getLogger()
    root_logger.handlers.clear()
    root_logger.addHandler(handler)
    root_logger.setLevel(log_level)
