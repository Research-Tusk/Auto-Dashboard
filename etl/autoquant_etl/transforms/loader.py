"""
AutoQuant ETL — Loader
=======================
Loads validated data into the warehouse:
  1. Bronze: Insert raw extraction records into raw_vahan_snapshot
  2. Silver: Insert daily delta records into fact_daily_registrations
             and upsert monthly aggregates into fact_monthly_registrations
  3. MV: Refresh mv_oem_monthly_summary
"""

from __future__ import annotations

from datetime import date
from typing import List

import asyncpg
import structlog

from autoquant_etl.connectors.base import RawRecord
from autoquant_etl.transforms.daily_delta import DailyDeltaRecord

logger = structlog.get_logger(__name__)


async def load_to_bronze(
    pool: asyncpg.Pool,
    run_id: int,
    records: List[RawRecord],
    period: str,
) -> int:
    """
    Load raw records into raw_vahan_snapshot (Bronze layer).

    Args:
        pool: asyncpg connection pool
        run_id: extraction run ID from raw_extraction_log
        records: raw records from VAHAN connector
        period: data period string 'YYYY-MM'

    Returns:
        Number of records inserted
    """
    if not records:
        return 0

    async with pool.acquire() as conn:
        await conn.executemany(
            """
            INSERT INTO raw_vahan_snapshot
                (run_id, data_period, state_filter, vehicle_class, fuel, maker, registration_count)
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            """,
            [
                (
                    run_id,
                    period,
                    rec.state or "ALL",
                    rec.vehicle_class,
                    rec.fuel,
                    rec.maker,
                    rec.registration_count,
                )
                for rec in records
            ],
        )

    logger.info("loader.bronze_loaded", run_id=run_id, count=len(records))
    return len(records)


async def load_to_silver(
    pool: asyncpg.Pool,
    run_id: int,
    daily_records: List[DailyDeltaRecord],
    month_key: date,
    mark_full_month: bool = False,
) -> int:
    """
    Load daily delta records into Silver layer.

    Inserts into:
      - fact_daily_registrations (one row per day × dimension combination)
      - fact_monthly_registrations (upserted monthly aggregate)

    Args:
        pool: asyncpg connection pool
        run_id: extraction run ID
        daily_records: output of compute_daily_delta()
        month_key: first day of the month
        mark_full_month: if True, set is_full_month=TRUE (use for backfill)

    Returns:
        Number of daily records inserted
    """
    if not daily_records:
        return 0

    async with pool.acquire() as conn:
        async with conn.transaction():
            # Insert daily facts
            await conn.executemany(
                """
                INSERT INTO fact_daily_registrations
                    (date_key, oem_id, segment_id, fuel_id, geo_id, run_id, registration_count, is_revision)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                ON CONFLICT (date_key, oem_id, segment_id, fuel_id, geo_id)
                DO UPDATE SET
                    registration_count = EXCLUDED.registration_count,
                    is_revision        = EXCLUDED.is_revision,
                    run_id             = EXCLUDED.run_id
                """,
                [
                    (
                        rec.date_key,
                        rec.oem_id,
                        rec.segment_id,
                        rec.fuel_id,
                        rec.geo_id,
                        run_id,
                        rec.registration_count,
                        rec.is_revision,
                    )
                    for rec in daily_records
                ],
            )

            # Upsert monthly aggregates
            await conn.execute(
                """
                INSERT INTO fact_monthly_registrations
                    (month_key, oem_id, segment_id, fuel_id, geo_id, units, mtd_as_of, is_full_month)
                SELECT
                    $1 AS month_key,
                    oem_id, segment_id, fuel_id, geo_id,
                    SUM(registration_count) AS units,
                    CURRENT_DATE AS mtd_as_of,
                    $2 AS is_full_month
                FROM fact_daily_registrations
                WHERE date_key >= $1
                  AND date_key < $1 + INTERVAL '1 month'
                GROUP BY oem_id, segment_id, fuel_id, geo_id
                ON CONFLICT (month_key, oem_id, segment_id, fuel_id, geo_id)
                DO UPDATE SET
                    units         = EXCLUDED.units,
                    mtd_as_of     = EXCLUDED.mtd_as_of,
                    is_full_month = GREATEST(fact_monthly_registrations.is_full_month, EXCLUDED.is_full_month),
                    updated_at    = NOW()
                """,
                month_key, mark_full_month,
            )

    logger.info("loader.silver_loaded", run_id=run_id, count=len(daily_records))
    return len(daily_records)


async def refresh_mv(pool: asyncpg.Pool) -> None:
    """Refresh the mv_oem_monthly_summary materialized view."""
    async with pool.acquire() as conn:
        await conn.execute("REFRESH MATERIALIZED VIEW CONCURRENTLY mv_oem_monthly_summary")
    logger.info("loader.mv_refreshed")
