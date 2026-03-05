"""
AutoQuant ETL — Historical Backfill
======================================
Runs the VAHAN extraction pipeline for a range of historical months.

Backfill strategy:
  1. Parse from_month and to_month (YYYY-MM format)
  2. Iterate month by month (chronological order)
  3. For each month: extract → validate → normalize → load
  4. Skip months already loaded (unless --force)
  5. Sleep between months (configurable, default 30s) to avoid rate limiting

The backfill reuses the same orchestrator steps as daily pipeline but:
  - Sets mark_full_month=True on load_to_silver (marks as complete month)
  - Uses a longer sleep between months

Usage:
    python -m autoquant_etl backfill --from-month 2025-04 --to-month 2025-12
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import date
from typing import List, Optional

import asyncpg
import structlog

from autoquant_etl.config import Settings
from autoquant_etl.connectors.vahan import VahanConnector
from autoquant_etl.connectors.base import ExtractParams
from autoquant_etl.transforms.normalize import normalize_records, load_dimension_lookups
from autoquant_etl.transforms.daily_delta import compute_daily_delta
from autoquant_etl.transforms.loader import load_to_bronze, load_to_silver, refresh_mv
from autoquant_etl.validators.gate import run_validation_gate, GateConfig

logger = structlog.get_logger(__name__)


@dataclass
class BackfillResult:
    """Result of a historical backfill run."""
    months_processed: int = 0
    months_skipped: int = 0
    records_loaded: int = 0
    failed_months: List[str] = None

    def __post_init__(self):
        if self.failed_months is None:
            self.failed_months = []


def _parse_month(month_str: str) -> date:
    """Parse 'YYYY-MM' string into a date (first of month)."""
    parts = month_str.strip().split("-")
    if len(parts) != 2:
        raise ValueError(f"Invalid month format: '{month_str}'. Use YYYY-MM")
    return date(int(parts[0]), int(parts[1]), 1)


def _month_range(from_month: date, to_month: date) -> List[date]:
    """Generate list of first-of-month dates from from_month to to_month inclusive."""
    months = []
    current = from_month
    while current <= to_month:
        months.append(current)
        # Advance to next month
        if current.month == 12:
            current = date(current.year + 1, 1, 1)
        else:
            current = date(current.year, current.month + 1, 1)
    return months


async def _is_month_loaded(pool: asyncpg.Pool, month: date) -> bool:
    """Check if the month already has data in fact_monthly_registrations."""
    async with pool.acquire() as conn:
        count = await conn.fetchval(
            "SELECT COUNT(*) FROM fact_monthly_registrations WHERE month_key = $1",
            month,
        )
    return int(count or 0) > 0


async def run_backfill(
    pool: asyncpg.Pool,
    settings: Settings,
    from_month: str,
    to_month: str,
    dry_run: bool = False,
    verbose: bool = False,
    force: bool = False,
) -> BackfillResult:
    """
    Run historical VAHAN data backfill for a range of months.

    Args:
        pool: asyncpg connection pool
        settings: application settings
        from_month: start month in 'YYYY-MM' format (inclusive)
        to_month: end month in 'YYYY-MM' format (inclusive)
        dry_run: if True, extract but skip DB writes
        verbose: enable verbose logging
        force: if True, re-extract even if month data already exists

    Returns:
        BackfillResult with counts of processed/skipped months and records
    """
    start = _parse_month(from_month)
    end = _parse_month(to_month)

    if start > end:
        raise ValueError(
            f"from_month ({from_month}) must be before or equal to to_month ({to_month})"
        )

    months = _month_range(start, end)
    result = BackfillResult()

    log = logger.bind(
        from_month=from_month,
        to_month=to_month,
        total_months=len(months),
        dry_run=dry_run,
        force=force,
    )
    log.info("backfill.start")

    # Pre-load dimension lookups once (reuse across months)
    dims = await load_dimension_lookups(pool)

    for month in months:
        period = month.strftime("%Y-%m")
        month_log = logger.bind(month=period)

        # Skip if already loaded (unless force)
        if not force and not dry_run:
            already_loaded = await _is_month_loaded(pool=pool, month=month)
            if already_loaded:
                month_log.info("backfill.month_skip", reason="already_loaded")
                result.months_skipped += 1
                continue

        month_log.info("backfill.month_start")
        run_id: Optional[int] = None

        try:
            # Create extraction log entry
            if not dry_run:
                async with pool.acquire() as conn:
                    run_id = await conn.fetchval(
                        """
                        INSERT INTO raw_extraction_log (source, status)
                        VALUES ('VAHAN_BACKFILL', 'RUNNING')
                        RETURNING run_id
                        """
                    )

            # Extract from VAHAN
            async with VahanConnector(settings) as vc:
                params = ExtractParams(
                    period=period,
                    period_type="month",
                    state="All India",
                    y_axis_types=["makerName", "vehicleClass"],
                )
                extraction_result = await vc.extract(params)

            records_extracted = len(extraction_result.records)
            month_log.info("backfill.extracted", records=records_extracted)

            if records_extracted == 0:
                raise ValueError("No records extracted from VAHAN")

            # Validate
            gate_result = await run_validation_gate(
                records=extraction_result.records,
                pool=pool,
                month=period,
                config=GateConfig(),
            )
            if not gate_result.passed:
                raise ValueError(
                    f"Validation gate failed: {', '.join(gate_result.failed_check_names)}"
                )

            if dry_run:
                month_log.info("backfill.dry_run_month", records=records_extracted)
                result.months_processed += 1
                continue

            # Normalize
            normalized = normalize_records(
                records=extraction_result.records,
                dims=dims,
                alert_unmapped=False,  # Suppress alerts during backfill
                settings=settings,
            )

            # Compute daily delta (for backfill: treat as month-end, day 1 logic)
            delta_records = compute_daily_delta(
                normalized_records=normalized.records,
                month_date=month,
            )

            # Load bronze
            await load_to_bronze(
                pool=pool,
                run_id=run_id,
                records=extraction_result.records,
                period=period,
            )

            # Load silver (mark_full_month=True for completed historical months)
            records_loaded = await load_to_silver(
                pool=pool,
                run_id=run_id,
                daily_records=delta_records,
                month_key=month,
                mark_full_month=True,
            )
            result.records_loaded += records_loaded

            # Mark run as SUCCESS
            async with pool.acquire() as conn:
                await conn.execute(
                    """
                    UPDATE raw_extraction_log
                    SET status='SUCCESS', completed_at=NOW(),
                        records_extracted=$1, records_loaded=$2
                    WHERE run_id=$3
                    """,
                    records_extracted,
                    records_loaded,
                    run_id,
                )

            result.months_processed += 1
            month_log.info("backfill.month_complete", records_loaded=records_loaded)

        except Exception as exc:
            month_log.error("backfill.month_failed", error=str(exc))
            result.failed_months.append(period)

            if run_id is not None:
                try:
                    async with pool.acquire() as conn:
                        await conn.execute(
                            """
                            UPDATE raw_extraction_log
                            SET status='FAILED', completed_at=NOW(), error_message=$1
                            WHERE run_id=$2
                            """,
                            str(exc),
                            run_id,
                        )
                except Exception:
                    pass

        # Sleep between months to avoid rate limiting (skip after last month)
        if month != months[-1]:
            sleep_secs = settings.backfill_sleep_seconds
            month_log.debug("backfill.sleep", seconds=sleep_secs)
            await asyncio.sleep(sleep_secs)

    # Refresh materialized view once at end (if anything was loaded)
    if result.months_processed > 0 and not dry_run:
        await refresh_mv(pool)
        log.info("backfill.mv_refreshed")

    log.info(
        "backfill.complete",
        months_processed=result.months_processed,
        months_skipped=result.months_skipped,
        records_loaded=result.records_loaded,
        failed_months=result.failed_months,
    )

    return result
