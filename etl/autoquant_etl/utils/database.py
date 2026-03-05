"""
AutoQuant ETL — Database Utilities
====================================
Async connection pool management using asyncpg.
"""

from __future__ import annotations

import asyncpg
import structlog

logger = structlog.get_logger(__name__)


async def get_pool(database_url: str) -> asyncpg.Pool:
    """
    Create and return an asyncpg connection pool.

    Args:
        database_url: PostgreSQL connection string
                      (postgresql://user:pw@host:port/db)

    Returns:
        asyncpg.Pool ready for use

    Raises:
        asyncpg.PostgresError: if connection cannot be established
    """
    # asyncpg requires 'postgresql://' scheme; normalise postgres:// aliases
    url = database_url
    if url.startswith("postgres://"):
        url = "postgresql://" + url[len("postgres://"):]

    pool = await asyncpg.create_pool(
        dsn=url,
        min_size=2,
        max_size=10,
        command_timeout=60,
        # Automatically apply codec for jsonb
        init=_init_connection,
    )
    logger.info("database.pool_created", min_size=2, max_size=10)
    return pool


async def close_pool(pool: asyncpg.Pool) -> None:
    """
    Gracefully close the connection pool.

    Args:
        pool: asyncpg.Pool to close
    """
    await pool.close()
    logger.info("database.pool_closed")


async def _init_connection(conn: asyncpg.Connection) -> None:
    """Per-connection initialisation: register JSONB codec."""
    await conn.set_type_codec(
        "jsonb",
        encoder=_jsonb_encode,
        decoder=_jsonb_decode,
        schema="pg_catalog",
        format="text",
    )


def _jsonb_encode(value: object) -> str:
    import json
    return json.dumps(value)


def _jsonb_decode(value: str) -> object:
    import json
    return json.loads(value)
