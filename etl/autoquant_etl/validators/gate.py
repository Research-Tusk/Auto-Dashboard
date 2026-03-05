"""
AutoQuant ETL — Validation Gate
=================================
Quality gate that validates extracted VAHAN data before it is loaded
into the warehouse. Runs a set of configurable checks.

Checks performed:
  1. record_count   — total records >= min_records
  2. no_negatives   — no negative registration counts
  3. min_makers     — unique maker names >= min_makers
  4. delta_check    — month-over-month record count delta within max_delta_pct

All checks that fail are collected; the gate passes only if all pass.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

import asyncpg
import structlog

from autoquant_etl.connectors.base import RawRecord

logger = structlog.get_logger(__name__)


@dataclass
class GateConfig:
    """Configurable thresholds for the validation gate."""
    min_records: int = 10
    max_delta_pct: float = 50.0   # maximum % change vs previous month record count
    min_makers: int = 5


@dataclass
class GateResult:
    """Result of running the validation gate."""
    passed: bool
    failed_check_names: List[str] = field(default_factory=list)
    details: Dict[str, Any] = field(default_factory=dict)


async def run_validation_gate(
    records: List[RawRecord],
    pool: asyncpg.Pool,
    month: str,
    config: Optional[GateConfig] = None,
) -> GateResult:
    """
    Run all validation checks on extracted records.

    Args:
        records: list of RawRecord from the connector
        pool: asyncpg pool (used for delta check DB query)
        month: data period in 'YYYY-MM' format
        config: optional gate configuration; defaults to GateConfig()

    Returns:
        GateResult with passed flag and list of failed check names
    """
    cfg = config or GateConfig()
    failed: List[str] = []
    details: Dict[str, Any] = {}

    # -------------------------------------------------------------------------
    # Check 1: Minimum record count
    # -------------------------------------------------------------------------
    record_count = len(records)
    details["record_count"] = record_count
    if record_count < cfg.min_records:
        failed.append("record_count")
        logger.warning(
            "gate.check_failed",
            check="record_count",
            actual=record_count,
            minimum=cfg.min_records,
        )
    else:
        logger.debug("gate.check_passed", check="record_count", count=record_count)

    # -------------------------------------------------------------------------
    # Check 2: No negative registration counts
    # -------------------------------------------------------------------------
    negative_count = sum(1 for r in records if r.registration_count < 0)
    details["negative_count"] = negative_count
    if negative_count > 0:
        failed.append("no_negatives")
        logger.warning(
            "gate.check_failed",
            check="no_negatives",
            negative_records=negative_count,
        )
    else:
        logger.debug("gate.check_passed", check="no_negatives")

    # -------------------------------------------------------------------------
    # Check 3: Minimum unique makers
    # -------------------------------------------------------------------------
    unique_makers = {r.maker for r in records if r.maker}
    maker_count = len(unique_makers)
    details["unique_makers"] = maker_count
    if maker_count < cfg.min_makers:
        failed.append("min_makers")
        logger.warning(
            "gate.check_failed",
            check="min_makers",
            actual=maker_count,
            minimum=cfg.min_makers,
        )
    else:
        logger.debug("gate.check_passed", check="min_makers", count=maker_count)

    # -------------------------------------------------------------------------
    # Check 4: Month-over-month delta
    # -------------------------------------------------------------------------
    try:
        prev_count = await _get_previous_month_record_count(pool=pool, month=month)
        details["prev_month_snapshot_count"] = prev_count

        if prev_count is not None and prev_count > 0:
            delta_pct = abs(record_count - prev_count) / prev_count * 100
            details["delta_pct"] = round(delta_pct, 2)
            if delta_pct > cfg.max_delta_pct:
                failed.append("delta_check")
                logger.warning(
                    "gate.check_failed",
                    check="delta_check",
                    delta_pct=round(delta_pct, 2),
                    max_allowed=cfg.max_delta_pct,
                    current=record_count,
                    previous=prev_count,
                )
            else:
                logger.debug(
                    "gate.check_passed",
                    check="delta_check",
                    delta_pct=round(delta_pct, 2),
                )
        else:
            details["delta_pct"] = None
            logger.info("gate.delta_check_skipped", reason="no_prior_month_data")

    except Exception as exc:
        # Non-fatal: delta check failure doesn't block the pipeline
        logger.warning("gate.delta_check_error", error=str(exc))
        details["delta_check_error"] = str(exc)

    passed = len(failed) == 0

    logger.info(
        "gate.result",
        passed=passed,
        failed_checks=failed,
        record_count=record_count,
        unique_makers=maker_count,
    )

    return GateResult(passed=passed, failed_check_names=failed, details=details)


async def _get_previous_month_record_count(
    pool: asyncpg.Pool,
    month: str,
) -> Optional[int]:
    """
    Query raw_vahan_snapshot for the previous month's record count.

    Args:
        pool: asyncpg pool
        month: current month in 'YYYY-MM' format

    Returns:
        Row count for the previous month, or None if not found
    """
    # Derive previous month string
    year, mon = map(int, month.split("-"))
    if mon == 1:
        prev_year, prev_mon = year - 1, 12
    else:
        prev_year, prev_mon = year, mon - 1

    prev_period = f"{prev_year:04d}-{prev_mon:02d}"

    async with pool.acquire() as conn:
        count = await conn.fetchval(
            """
            SELECT COUNT(*)
            FROM raw_vahan_snapshot
            WHERE data_period = $1
            """,
            prev_period,
        )
    return int(count) if count is not None else None
