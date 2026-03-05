"""
AutoQuant ETL — SQL Migration Runner
======================================
Applies ordered SQL migration files from etl/autoquant_etl/migrations/.
Tracks applied migrations in a _migrations table in the database.

Migration files must be named with a numeric prefix for ordering:
  001_initial_schema.sql
  002_add_fada_table.sql
  ...

Migrations are idempotent: already-applied migrations are skipped.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass, field
from pathlib import Path
from typing import List, Optional

import asyncpg
import structlog

logger = structlog.get_logger(__name__)

# Directory containing migration SQL files (relative to this file's package root)
MIGRATIONS_DIR = Path(__file__).parent.parent / "migrations"

# DDL to create the migration tracking table
CREATE_MIGRATIONS_TABLE = """
CREATE TABLE IF NOT EXISTS _migrations (
    id           SERIAL PRIMARY KEY,
    filename     TEXT NOT NULL UNIQUE,
    checksum     TEXT NOT NULL,
    applied_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
"""


@dataclass
class MigrationResult:
    """Result of a migration run."""
    applied: List[str] = field(default_factory=list)
    skipped: List[str] = field(default_factory=list)
    dry_run: bool = False


async def run_migrations(
    pool: asyncpg.Pool,
    dry_run: bool = False,
    verbose: bool = False,
    migrations_dir: Optional[Path] = None,
) -> MigrationResult:
    """
    Discover and apply pending SQL migrations.

    Reads .sql files from etl/autoquant_etl/migrations/ (or override),
    orders them lexicographically (so numeric prefixes determine order),
    and applies any that have not yet been recorded in the _migrations table.

    Args:
        pool: asyncpg connection pool
        dry_run: if True, print migrations that would be applied without executing
        verbose: enable verbose logging per migration
        migrations_dir: override default migrations directory path

    Returns:
        MigrationResult with lists of applied and skipped migration filenames
    """
    result = MigrationResult(dry_run=dry_run)
    directory = migrations_dir or MIGRATIONS_DIR

    # Ensure migrations directory exists
    if not directory.exists():
        logger.warning("migrations.dir_not_found", path=str(directory))
        directory.mkdir(parents=True, exist_ok=True)
        return result

    # Collect and sort migration files
    sql_files = sorted(directory.glob("*.sql"))
    if not sql_files:
        logger.info("migrations.no_files", directory=str(directory))
        return result

    async with pool.acquire() as conn:
        # Ensure tracking table exists
        await conn.execute(CREATE_MIGRATIONS_TABLE)

        # Load already-applied migrations
        applied_rows = await conn.fetch("SELECT filename FROM _migrations ORDER BY filename")
        applied_set = {row["filename"] for row in applied_rows}

        for sql_file in sql_files:
            filename = sql_file.name

            if filename in applied_set:
                result.skipped.append(filename)
                if verbose:
                    logger.debug("migrations.skip", filename=filename)
                continue

            sql_content = sql_file.read_text(encoding="utf-8")
            checksum = hashlib.sha256(sql_content.encode()).hexdigest()

            if dry_run:
                logger.info("migrations.would_apply", filename=filename)
                result.applied.append(filename)
                continue

            try:
                async with conn.transaction():
                    await conn.execute(sql_content)
                    await conn.execute(
                        "INSERT INTO _migrations (filename, checksum) VALUES ($1, $2)",
                        filename,
                        checksum,
                    )
                result.applied.append(filename)
                logger.info("migrations.applied", filename=filename, checksum=checksum[:8])
            except Exception as exc:
                logger.error(
                    "migrations.failed",
                    filename=filename,
                    error=str(exc),
                )
                raise RuntimeError(f"Migration '{filename}' failed: {exc}") from exc

    logger.info(
        "migrations.complete",
        applied=len(result.applied),
        skipped=len(result.skipped),
        dry_run=dry_run,
    )
    return result
