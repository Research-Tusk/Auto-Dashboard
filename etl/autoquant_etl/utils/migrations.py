"""
AutoQuant ETL — DB Migration Runner
=====================================
Applies pending SQL migration files in order.

Migration files live in etl/autoquant_etl/migrations/ and are named:
  001_initial_schema.sql
  002_add_heartbeat_table.sql
  ... etc.

Applied migrations are tracked in a `_migrations` table in the DB.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import List

import asyncpg
import structlog

logger = structlog.get_logger(__name__)

MIGRATIONS_DIR = Path(__file__).parent.parent / "migrations"
MIGRATIONS_TABLE = "_migrations"


@dataclass
class MigrationResult:
    applied: List[str] = field(default_factory=list)
    skipped: List[str] = field(default_factory=list)


async def _ensure_migrations_table(conn: asyncpg.Connection) -> None:
    """Create _migrations tracking table if it doesn't exist."""
    await conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {MIGRATIONS_TABLE} (
            migration_name VARCHAR(255) PRIMARY KEY,
            applied_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
        """
    )


async def _get_applied_migrations(conn: asyncpg.Connection) -> set:
    """Return set of already-applied migration names."""
    rows = await conn.fetch(f"SELECT migration_name FROM {MIGRATIONS_TABLE}")
    return {r["migration_name"] for r in rows}


async def run_migrations(
    pool: asyncpg.Pool,
    dry_run: bool = False,
    verbose: bool = False,
) -> MigrationResult:
    """
    Apply all pending SQL migration files.

    Args:
        pool: asyncpg connection pool
        dry_run: if True, show pending migrations without applying
        verbose: enable verbose logging

    Returns:
        MigrationResult with lists of applied and skipped migrations
    """
    result = MigrationResult()

    if not MIGRATIONS_DIR.exists():
        logger.warning("migrations.dir_not_found", path=str(MIGRATIONS_DIR))
        return result

    # Find all .sql files in migrations directory, sorted
    migration_files = sorted(MIGRATIONS_DIR.glob("*.sql"))
    if not migration_files:
        logger.info("migrations.no_files_found")
        return result

    async with pool.acquire() as conn:
        await _ensure_migrations_table(conn)
        applied = await _get_applied_migrations(conn)

        for migration_file in migration_files:
            name = migration_file.name

            if name in applied:
                result.skipped.append(name)
                if verbose:
                    logger.info("migrations.skip", migration=name)
                continue

            sql = migration_file.read_text(encoding="utf-8")

            if dry_run:
                result.applied.append(name)
                logger.info("migrations.dry_run", migration=name)
                continue

            try:
                async with conn.transaction():
                    await conn.execute(sql)
                    await conn.execute(
                        f"INSERT INTO {MIGRATIONS_TABLE} (migration_name) VALUES ($1)",
                        name,
                    )
                result.applied.append(name)
                logger.info("migrations.applied", migration=name)
            except Exception as exc:
                logger.error("migrations.failed", migration=name, error=str(exc))
                raise

    return result
