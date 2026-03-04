"""
AutoQuant ETL — Historical Backfill Orchestrator
===================================================
Iterates over a range of past months and runs the full pipeline
(VAHAN extract → normalize → delta → load) for each month.

Design decisions:
  - Skips months that already have is_full_month=TRUE unless force=True
  - Writes a local progress file so re-runs are idempotent
  - One extraction run per month — does NOT re-extract each day
  - Sleeps between months to be polite to VAHAN server
"""

from __future__ import annotations

import asyncio
import json
import logging
from dataclasses import dataclass, field
from datetime import date, timedelta
from pathlib import Path
from typing import List, Optional

import asyncpg
import structlog

from autoquant_etl.config import Settings
from autoquant_etl.connectors.vahan import VahanConnector
from autoquant_etl.transforms.normalize import normalize_records, load_dimension_lookups
from autoquant_etl.transforms.daily_delta import compute_daily_delta
from autoquant_etl.transforms.loader import load_to_bronze, load_to_silver
from autoquant_etl.validators.gate import run_validation_gate, GateConfig
from autoquant_etl.utils.alerts import send_telegram_alert
from autoquant_etl.utils.fy_calendar import month_to_fy_quarter

logger = structlog.get_logger(__name__)

PROGRESS_FILE = Path("autoquant_backfill_progress.json")
SLEEP_BETWEEN_MONTHS = 30  # seconds; be polite to VAHAN


@dataclass
class BackfillMonthResult:
    month: str           # 'YYYY-MM'
    skipped: bool = False
    success: bool = False
    records_extracted: int = 0
    records_loaded: int = 0
    error: Optional[str] = None


@dataclass
class BackfillResult:
    months_processed: int = 0
    months_skipped: int = 0
    records_loaded: int = 0
    errors: List[str] = field(default_factory=list)
    month_results: List[BackfillMonthResult] = field(default_factory=list)


def _month_range(from_month: str, to_month: str) -> List[str]:
    """Generate list of 'YYYY-MM' strings from from_month to to_month inclusive."""
    start_year, start_mon = map(int, from_month.split("-"))
    end_year, end_mon = map(int, to_month.split("-"))

    months = []
    year, mon = start_year, start_mon
    while (year, mon) <= (end_year, end_mon):
        months.append(f"{year:04d}-{mon:02d}")
        mon += 1
        if mon > 12:
            mon = 1
            year += 1
    return months


def _load_progress() -> dict:
    if PROGRESS_FILE.exists():
        try:
            return json.loads(PROGRESS_FILE.read_text())
        except Exception:
            return {}
    return {}


def _save_progress(progress: dict) -> None:
    try:
        PROGRESS_FILE.write_text(json.dumps(progress, indent=2))
    except Exception as exc:
        logger.warning("backfill.progress_save_failed", error=str(exc))


async def _month_has_full_data(pool: asyncpg.Pool, month: str) -> bool:
    """Return True if the month already has is_full_month=TRUE in fact_monthly_registrations."""
    year, mon = map(int, month.split("-"))
    month_date = date(year, mon, 1)
    async with pool.acquire() as conn:
        count = await conn.fetchval(
            "SELECT COUNT(*) FROM fact_monthly_registrations "
            "WHERE month_key = $1 AND is_full_month = TRUE",
            month_date,
        )
    return (count or 0) > 0


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
    Run historical backfill for months in [from_month, to_month].

    Args:
        pool: asyncpg connection pool
        settings: application settings
        from_month: start month 'YYYY-MM'
        to_month: end month 'YYYY-MM' (inclusive)
        dry_run: if True, extract but don't write to DB
        verbose: enable verbose logging
        force: re-extract even if data exists

    Returns:
        BackfillResult with counts and per-month results
    """
    months = _month_range(from_month, to_month)
    progress = _load_progress()
    result = BackfillResult()

    log = logger.bind(
        from_month=from_month,
        to_month=to_month,
        total_months=len(months),
        dry_run=dry_run,
        force=force,
    )
    log.info("backfill.start")

    # Load dimension lookups once
    dims = await load_dimension_lookups(pool)

    for i, month in enumerate(months):
        month_log = log.bind(month=month, progress=f"{i+1}/{len(months)}")

        # Check if already done
        if not force and progress.get(month) == "done":
            month_log.info("backfill.month_skip", reason="progress_file")
            result.months_skipped += 1
            result.month_results.append(BackfillMonthResult(month=month, skipped=True))
            continue

        if not force and await _month_has_full_data(pool, month):
            month_log.info("backfill.month_skip", reason="already_full_in_db")
            result.months_skipped += 1
            progress[month] = "done"
            _save_progress(progress)
            result.month_results.append(BackfillMonthResult(month=month, skipped=True))
            continue

        month_result = BackfillMonthResult(month=month)

        try:
            month_log.info("backfill.month_start")

            # 1. Extract from VAHAN
            async with VahanConnector(settings) as vc:
                from autoquant_etl.connectors.base import ExtractParams
                params = ExtractParams(
                    period=month,
                    period_type="month",
                    state="All India",
                    y_axis_types=["makerName", "vehicleClass"],
                )
                extraction_result = await vc.extract(params)

            if not extraction_result.records:
                month_log.warning("backfill.month_empty")
                month_result.skipped = True
                result.month_results.append(month_result)
                continue

            month_result.records_extracted = len(extraction_result.records)
            month_log.info("backfill.extracted", count=len(extraction_result.records))

            # 2. Validate
            gate_cfg = GateConfig(min_row_count=50, allow_zero_fuel=True)
            gate_result = await run_validation_gate(
                records=extraction_result.records,
                pool=pool,
                month=month,
                config=gate_cfg,
            )
            if not gate_result.passed:
                month_log.warning("backfill.gate_failed", checks=gate_result.failed_check_names)
                await send_telegram_alert(
                    settings=settings,
                    message=(
                        f"⚠️ Backfill QA gate failed for {month}: "
                        f"{', '.join(gate_result.failed_check_names)}"
                    ),
                )
                month_result.error = f"Validation gate failed: {gate_result.failed_check_names}"
                result.errors.append(month_result.error)
                result.month_results.append(month_result)
                continue

            if dry_run:
                month_log.info("backfill.dry_run_skip")
                month_result.success = True
                result.months_processed += 1
                result.month_results.append(month_result)
                continue

            # 3. Normalize
            normalized = normalize_records(
                records=extraction_result.records,
                dims=dims,
                alert_unmapped=True,
                settings=settings,
            )

            # 4. Create extraction log entry
            async with pool.acquire() as conn:
                run_id = await conn.fetchval(
                    """
                    INSERT INTO raw_extraction_log
                        (source, status, records_extracted, notes)
                    VALUES ('VAHAN', 'RUNNING', $1, $2)
                    RETURNING run_id
                    """,
                    len(extraction_result.records),
                    f"Backfill: {month}",
                )

            # 5. Load bronze
            await load_to_bronze(
                pool=pool,
                run_id=run_id,
                records=extraction_result.records,
                period=month,
            )

            # 6. Compute daily delta & load silver
            year, mon = map(int, month.split("-"))
            month_date = date(year, mon, 1)
            delta_records = compute_daily_delta(
                normalized_records=normalized.records,
                month_date=month_date,
            )

            loaded = await load_to_silver(
                pool=pool,
                run_id=run_id,
                daily_records=delta_records,
                month_key=month_date,
                mark_full_month=True,  # Backfill always marks as full month
            )
            month_result.records_loaded = loaded
            result.records_loaded += loaded

            # 7. Mark extraction log as success
            async with pool.acquire() as conn:
                await conn.execute(
                    """
                    UPDATE raw_extraction_log
                    SET status='SUCCESS', completed_at=NOW(), records_loaded=$1
                    WHERE run_id=$2
                    """,
                    loaded,
                    run_id,
                )

            month_result.success = True
            result.months_processed += 1
            progress[month] = "done"
            _save_progress(progress)
            month_log.info("backfill.month_done", loaded=loaded)

        except Exception as exc:
            month_log.error("backfill.month_error", error=str(exc))
            month_result.error = str(exc)
            result.errors.append(f"{month}: {exc}")
            await send_telegram_alert(
                settings=settings,
                message=f"❌ Backfill failed for {month}: {exc}",
            )

        result.month_results.append(month_result)

        # Sleep between months (skip on last month)
        if i < len(months) - 1 and not dry_run:
            await asyncio.sleep(SLEEP_BETWEEN_MONTHS)

    log.info(
        "backfill.complete",
        processed=result.months_processed,
        skipped=result.months_skipped,
        loaded=result.records_loaded,
        errors=len(result.errors),
    )
    return result
