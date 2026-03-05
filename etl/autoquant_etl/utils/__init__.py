"""
AutoQuant ETL — Utils Package
================================
Re-exports key utility functions for convenient importing.
"""

from autoquant_etl.utils.database import get_pool, close_pool
from autoquant_etl.utils.logging import configure_logging
from autoquant_etl.utils.alerts import send_telegram_alert, send_pipeline_digest
from autoquant_etl.utils.fy_calendar import (
    date_to_fy,
    date_to_fy_quarter,
    fy_quarter_date_range,
    current_fy_quarter,
)
from autoquant_etl.utils.migrations import run_migrations, MigrationResult
from autoquant_etl.utils.seeder import run_seed

__all__ = [
    "get_pool",
    "close_pool",
    "configure_logging",
    "send_telegram_alert",
    "send_pipeline_digest",
    "date_to_fy",
    "date_to_fy_quarter",
    "fy_quarter_date_range",
    "current_fy_quarter",
    "run_migrations",
    "MigrationResult",
    "run_seed",
]
