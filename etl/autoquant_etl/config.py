"""
AutoQuant ETL — Configuration Management
Loads settings from environment variables / .env file.
"""

from __future__ import annotations

import os
from enum import Enum
from pathlib import Path
from typing import Optional

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class LogLevel(str, Enum):
    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # ---------------------------------------------------------------------------
    # Database
    # ---------------------------------------------------------------------------
    database_url: str = Field(
        ...,
        description="PostgreSQL connection string. Format: postgresql://user:pw@host:port/db",
    )

    # ---------------------------------------------------------------------------
    # Telegram Alerts
    # ---------------------------------------------------------------------------
    telegram_bot_token: Optional[str] = Field(
        default=None,
        description="Telegram bot token. If not set, alerts are logged only.",
    )
    telegram_chat_id: Optional[str] = Field(
        default=None,
        description="Telegram chat or channel ID.",
    )

    # ---------------------------------------------------------------------------
    # ETL Behaviour
    # ---------------------------------------------------------------------------
    dry_run: bool = Field(
        default=False,
        description="If True, extract but skip DB writes. Useful for testing.",
    )
    log_level: LogLevel = Field(
        default=LogLevel.INFO,
        description="Logging level.",
    )
    playwright_headless: bool = Field(
        default=True,
        description="Run Playwright browser headless. Set False for local debugging.",
    )
    vahan_base_url: str = Field(
        default="https://vahan.parivahan.gov.in/vahan4dashboard/",
        description="VAHAN dashboard base URL. Override if mirror changes.",
    )
    backfill_sleep_seconds: int = Field(
        default=30,
        description="Seconds to sleep between backfill months.",
    )

    @field_validator("database_url")
    @classmethod
    def validate_database_url(cls, v: str) -> str:
        if not v.startswith(("postgresql://", "postgres://")):
            raise ValueError("DATABASE_URL must start with postgresql:// or postgres://")
        return v
