"""
AutoQuant ETL — pytest fixtures
"""

from __future__ import annotations

import asyncio
from typing import AsyncGenerator
from unittest.mock import AsyncMock, MagicMock

import asyncpg
import pytest

from autoquant_etl.config import Settings
from autoquant_etl.connectors.base import RawRecord, ExtractionResult, ConnectorSource


@pytest.fixture(scope="session")
def event_loop():
    """Create an event loop for the test session."""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()


@pytest.fixture
def settings() -> Settings:
    """Test settings with fake DB URL."""
    return Settings(
        database_url="postgresql://test:test@localhost:5432/test",
        telegram_bot_token=None,
        telegram_chat_id=None,
        dry_run=True,
        log_level="DEBUG",
    )


@pytest.fixture
def mock_pool() -> AsyncMock:
    """Mock asyncpg pool."""
    pool = AsyncMock(spec=asyncpg.Pool)
    conn = AsyncMock(spec=asyncpg.Connection)
    pool.acquire.return_value.__aenter__ = AsyncMock(return_value=conn)
    pool.acquire.return_value.__aexit__ = AsyncMock(return_value=False)
    return pool


@pytest.fixture
def sample_raw_records() -> list[RawRecord]:
    """Sample VAHAN raw records for testing."""
    return [
        RawRecord(
            maker="MARUTI SUZUKI INDIA LTD",
            vehicle_class="Motor Car",
            fuel="PETROL",
            registration_count=120000,
            period="2025-12",
            state="All India",
        ),
        RawRecord(
            maker="HYUNDAI MOTOR INDIA LTD",
            vehicle_class="Motor Car",
            fuel="PETROL",
            registration_count=50000,
            period="2025-12",
            state="All India",
        ),
        RawRecord(
            maker="TATA MOTORS LTD",
            vehicle_class="Motor Car",
            fuel="ELECTRIC(BOV)",
            registration_count=15000,
            period="2025-12",
            state="All India",
        ),
        RawRecord(
            maker="HERO MOTOCORP LTD",
            vehicle_class="M-CYCLE/SCOOTER",
            fuel="PETROL",
            registration_count=500000,
            period="2025-12",
            state="All India",
        ),
        RawRecord(
            maker="TATA MOTORS LTD",
            vehicle_class="Heavy Goods Vehicle",
            fuel="DIESEL",
            registration_count=30000,
            period="2025-12",
            state="All India",
        ),
    ]


@pytest.fixture
def extraction_result(sample_raw_records) -> ExtractionResult:
    """Sample ExtractionResult."""
    return ExtractionResult(
        source=ConnectorSource.VAHAN,
        period="2025-12",
        records=sample_raw_records,
    )
