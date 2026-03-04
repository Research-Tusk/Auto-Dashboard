"""
AutoQuant ETL — Dimension Table Seeder
Seeds dim_* tables from the SQL seed files in db/
"""

from __future__ import annotations

from pathlib import Path

import asyncpg
import structlog

logger = structlog.get_logger(__name__)

# Seed files relative to repo root (etl/ is the working dir when running)
SEED_FILES = [
    Path("../db/002_seed_dimensions_v2.sql"),
    Path("../db/003_seed_asp.sql"),
]

# Check markers: query to verify if seeding is needed
SEED_CHECK_QUERIES = [
    "SELECT COUNT(*) FROM dim_oem",
    "SELECT COUNT(*) FROM fact_asp_master",
]


async def run_seed(
    pool: asyncpg.Pool,
    force: bool = False,
    verbose: bool = False,
) -> None:
    """
    Seed dimension tables from SQL files.

    Skips seeding if tables already have data (unless force=True).

    Args:
        pool: asyncpg connection pool
        force: re-run seed even if tables already have data
        verbose: enable verbose logging
    """
    async with pool.acquire() as conn:
        # Check if already seeded
        if not force:
            already_seeded = True
            for check_query in SEED_CHECK_QUERIES:
                try:
                    count = await conn.fetchval(check_query)
                    if count == 0:
                        already_seeded = False
                        break
                except Exception:
                    already_seeded = False
                    break

            if already_seeded:
                logger.info("seeder.already_seeded")
                return

        # Apply seed files
        for seed_file in SEED_FILES:
            if not seed_file.exists():
                logger.warning("seeder.file_not_found", path=str(seed_file))
                continue

            sql = seed_file.read_text(encoding="utf-8")

            try:
                async with conn.transaction():
                    await conn.execute(sql)
                logger.info("seeder.applied", file=seed_file.name)
            except Exception as exc:
                logger.error("seeder.failed", file=seed_file.name, error=str(exc))
                raise

    logger.info("seeder.complete")
