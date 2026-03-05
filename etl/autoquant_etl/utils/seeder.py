"""
AutoQuant ETL — Dimension Seeder
==================================
Executes seed SQL files from etl/seeds/ to populate dimension tables.

Guard: checks dim_oem row count before seeding to avoid re-seeding
an already-populated database. Use --force to override.

Seed files must be in the etl/seeds/ directory and will be executed
in lexicographic order.
"""

from __future__ import annotations

from pathlib import Path
from typing import Optional

import asyncpg
import structlog

logger = structlog.get_logger(__name__)

# Seeds directory: etl/seeds/ relative to repo root
# When running from inside the etl/ package, resolve via two levels up
SEEDS_DIR = Path(__file__).parent.parent.parent.parent / "seeds"

# Threshold: if dim_oem already has this many rows, skip seeding
ALREADY_SEEDED_THRESHOLD = 1


async def run_seed(
    pool: asyncpg.Pool,
    force: bool = False,
    verbose: bool = False,
    seeds_dir: Optional[Path] = None,
) -> None:
    """
    Execute seed SQL files to populate dimension tables.

    Checks dim_oem row count first. If rows exist and force=False,
    skips seeding (idempotent behaviour).

    Args:
        pool: asyncpg connection pool
        force: if True, re-run seeds even if data already exists
        verbose: enable verbose per-file logging
        seeds_dir: override default seeds directory path
    """
    directory = seeds_dir or SEEDS_DIR

    async with pool.acquire() as conn:
        # Guard: check if already seeded
        try:
            oem_count: int = await conn.fetchval("SELECT COUNT(*) FROM dim_oem")
        except asyncpg.UndefinedTableError:
            logger.warning("seeder.dim_oem_missing")
            oem_count = 0

        if oem_count >= ALREADY_SEEDED_THRESHOLD and not force:
            logger.info(
                "seeder.already_seeded",
                dim_oem_count=oem_count,
                hint="Use --force to re-seed",
            )
            return

        if not directory.exists():
            logger.warning("seeder.dir_not_found", path=str(directory))
            return

        seed_files = sorted(directory.glob("*.sql"))
        if not seed_files:
            logger.info("seeder.no_files", directory=str(directory))
            return

        for seed_file in seed_files:
            sql_content = seed_file.read_text(encoding="utf-8")
            if verbose:
                logger.debug("seeder.executing", filename=seed_file.name)
            try:
                async with conn.transaction():
                    await conn.execute(sql_content)
                logger.info("seeder.executed", filename=seed_file.name)
            except Exception as exc:
                logger.error(
                    "seeder.failed",
                    filename=seed_file.name,
                    error=str(exc),
                )
                raise RuntimeError(f"Seed file '{seed_file.name}' failed: {exc}") from exc

    logger.info("seeder.complete", files_executed=len(seed_files))
