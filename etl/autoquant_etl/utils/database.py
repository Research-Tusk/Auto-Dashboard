"""
AutoQuant ETL — Database Connection Pool
Manages asyncpg connection pools.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Optional

import asyncpg
import structlog

logger = structlog.get_logger(__name__)

_pool: Optional[asyncpg.Pool] = None
_pool_lock = asyncio.Lock()

# Connection pool settings
MIN_POOL_SIZE = 2
MAX_POOL_SIZE = 10
COMMAND_TIMEOUT = 60  # seconds


async def get_pool(database_url: str) -> asyncpg.Pool:
    """
    Get or create a shared asyncpg connection pool.

    Uses a module-level singleton pool. Thread-safe via asyncio.Lock.

    Args:
        database_url: PostgreSQL connection string

    Returns:
        asyncpg.Pool instance

    Raises:
        asyncpg.PostgresError: if connection fails
    """
    global _pool

    async with _pool_lock:
        if _pool is None:
            logger.info("database.pool_creating", min_size=MIN_POOL_SIZE, max_size=MAX_POOL_SIZE)
            _pool = await asyncpg.create_pool(
                dsn=database_url,
                min_size=MIN_POOL_SIZE,
                max_size=MAX_POOL_SIZE,
                command_timeout=COMMAND_TIMEOUT,
                # SSL required for Supabase/Neon
                ssl="require",
                statement_cache_size=0,  # Disable for pgBouncer compatibility
            )
            logger.info("database.pool_created")

    return _pool


async def close_pool(pool: Optional[asyncpg.Pool] = None) -> None:
    """
    Close the connection pool.

    Args:
        pool: pool to close (if None, closes the module-level singleton)
    """
    global _pool

    target = pool or _pool
    if target is None:
        return

    await target.close()
    if target is _pool:
        _pool = None
    logger.info("database.pool_closed")


async def check_db_health(pool: asyncpg.Pool) -> bool:
    """
    Lightweight DB health check.

    Args:
        pool: asyncpg connection pool

    Returns:
        True if DB is healthy, False otherwise
    """
    try:
        async with pool.acquire() as conn:
            await conn.fetchval("SELECT 1")
        return True
    except Exception as exc:
        logger.error("database.health_check_failed", error=str(exc))
        return False


async def execute_with_retry(
    pool: asyncpg.Pool,
    query: str,
    *args,
    max_retries: int = 3,
    retry_delay: float = 1.0,
):
    """
    Execute a query with retry logic for transient failures.

    Args:
        pool: asyncpg connection pool
        query: SQL query string
        *args: Query parameters
        max_retries: Maximum number of retry attempts
        retry_delay: Base delay between retries (exponential backoff)

    Returns:
        Query result
    """
    for attempt in range(1, max_retries + 1):
        try:
            async with pool.acquire() as conn:
                return await conn.execute(query, *args)
        except asyncpg.TooManyConnectionsError as exc:
            if attempt == max_retries:
                raise
            logger.warning(
                "database.too_many_connections",
                attempt=attempt,
                delay=retry_delay * attempt,
            )
            await asyncio.sleep(retry_delay * attempt)
        except asyncpg.PostgresConnectionError as exc:
            if attempt == max_retries:
                raise
            logger.warning(
                "database.connection_error",
                attempt=attempt,
                error=str(exc),
                delay=retry_delay * attempt,
            )
            await asyncio.sleep(retry_delay * attempt)
