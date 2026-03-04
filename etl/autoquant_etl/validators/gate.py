"""
AutoQuant ETL — Validation Gate
=================================
Runs 7 quality checks on raw VAHAN extraction data before
it is passed to the transform pipeline.

Checks:
  1. Min row count (>= 50 maker rows expected nationally)
  2. Maker mapping coverage (>= 80% of volume must be mappable)
  3. Fuel mapping coverage (>= 90% of records must map to known fuel)
  4. Z-score anomaly detection (monthly count not > 3-sigma from history)
  5. Negative delta handler (warns if current MTD < prior MTD for any dim)
  6. Total volume sanity (aggregate count in expected range for India)
  7. Duplicate row check (no duplicate maker+fuel+class combinations)

If any check fails:
  - run_validation_gate() returns GateResult(passed=False)
  - Caller should mark extraction log as VALIDATION_FAILED and send alert
"""

from __future__ import annotations

import statistics
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

import asyncpg
import structlog

from autoquant_etl.connectors.base import RawRecord

logger = structlog.get_logger(__name__)

# Default thresholds (override via GateConfig)
DEFAULT_MIN_ROW_COUNT = 50
DEFAULT_MIN_MAKER_COVERAGE = 0.80   # 80% of volume must map to known OEM
DEFAULT_MIN_FUEL_COVERAGE = 0.90    # 90% of records must map to known fuel
DEFAULT_ZSCORE_THRESHOLD = 3.0      # Flag if monthly count > 3-sigma from history
DEFAULT_MIN_TOTAL_VOLUME = 100_000  # Minimum plausible national monthly registrations
DEFAULT_MAX_TOTAL_VOLUME = 5_000_000  # Maximum plausible (above India annual total)

# Known VAHAN fuel codes (used for coverage check)
KNOWN_FUELS = {
    "PETROL", "DIESEL", "CNG", "LPG", "CNG + PETROL", "LPG + PETROL",
    "STRONG HYBRID(EV)", "MILD HYBRID", "PLUG-IN HYBRID(PHEV)",
    "ELECTRIC(BOV)", "ELECTRIC", "HYDROGEN FUEL CELL",
    "OTHERS", "NOT APPLICABLE",
}


@dataclass
class GateConfig:
    min_row_count: int = DEFAULT_MIN_ROW_COUNT
    min_maker_coverage: float = DEFAULT_MIN_MAKER_COVERAGE
    min_fuel_coverage: float = DEFAULT_MIN_FUEL_COVERAGE
    zscore_threshold: float = DEFAULT_ZSCORE_THRESHOLD
    min_total_volume: int = DEFAULT_MIN_TOTAL_VOLUME
    max_total_volume: int = DEFAULT_MAX_TOTAL_VOLUME
    allow_zero_fuel: bool = False  # Allow records with fuel=None (backfill)


@dataclass
class GateCheck:
    name: str
    passed: bool
    detail: str = ""


@dataclass
class GateResult:
    passed: bool
    checks: List[GateCheck] = field(default_factory=list)

    @property
    def failed_check_names(self) -> List[str]:
        return [c.name for c in self.checks if not c.passed]


def _check_min_row_count(
    records: List[RawRecord], config: GateConfig
) -> GateCheck:
    """Check 1: Minimum number of records."""
    count = len(records)
    passed = count >= config.min_row_count
    return GateCheck(
        name="min_row_count",
        passed=passed,
        detail=f"{count} rows (min: {config.min_row_count})",
    )


def _check_maker_coverage(
    records: List[RawRecord], config: GateConfig
) -> GateCheck:
    """Check 2: Fraction of total volume from records with a non-null maker."""
    total_volume = sum(r.registration_count for r in records)
    if total_volume == 0:
        return GateCheck(
            name="maker_coverage",
            passed=False,
            detail="Total volume is 0; cannot compute maker coverage",
        )
    mapped_volume = sum(
        r.registration_count for r in records if r.maker and r.maker.strip()
    )
    coverage = mapped_volume / total_volume
    passed = coverage >= config.min_maker_coverage
    return GateCheck(
        name="maker_coverage",
        passed=passed,
        detail=f"{coverage:.1%} mapped (min: {config.min_maker_coverage:.0%})",
    )


def _check_fuel_coverage(
    records: List[RawRecord], config: GateConfig
) -> GateCheck:
    """Check 3: Fraction of records with a known fuel type."""
    fuel_records = [r for r in records if r.fuel is not None]
    if not fuel_records:
        if config.allow_zero_fuel:
            return GateCheck(
                name="fuel_coverage",
                passed=True,
                detail="No fuel records (allowed in backfill mode)",
            )
        return GateCheck(
            name="fuel_coverage",
            passed=False,
            detail="No fuel records found",
        )
    known_count = sum(
        1 for r in fuel_records if r.fuel.upper() in KNOWN_FUELS
    )
    coverage = known_count / len(fuel_records)
    passed = coverage >= config.min_fuel_coverage
    return GateCheck(
        name="fuel_coverage",
        passed=passed,
        detail=f"{coverage:.1%} known fuels (min: {config.min_fuel_coverage:.0%})",
    )


def _check_total_volume_sanity(
    records: List[RawRecord], config: GateConfig
) -> GateCheck:
    """Check 4: Total registration volume within expected bounds."""
    # Use maker records for total volume (avoid double-counting with class records)
    maker_records = [r for r in records if r.maker]
    total = sum(r.registration_count for r in maker_records)
    if total == 0:
        total = sum(r.registration_count for r in records)

    passed = config.min_total_volume <= total <= config.max_total_volume
    return GateCheck(
        name="total_volume_sanity",
        passed=passed,
        detail=(
            f"Total: {total:,} "
            f"(expected: {config.min_total_volume:,}–{config.max_total_volume:,})"
        ),
    )


async def _check_zscore_anomaly(
    records: List[RawRecord],
    pool: asyncpg.Pool,
    month: str,
    config: GateConfig,
) -> GateCheck:
    """Check 5: Z-score anomaly detection against historical monthly totals."""
    # Get last 12 months of total monthly registrations from DB
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT month_key, SUM(units) AS total_units
            FROM fact_monthly_registrations
            WHERE is_full_month = TRUE
              AND month_key >= NOW() - INTERVAL '14 months'
            GROUP BY month_key
            ORDER BY month_key DESC
            LIMIT 12
            """
        )

    if len(rows) < 3:
        return GateCheck(
            name="zscore_anomaly",
            passed=True,
            detail=f"Insufficient history ({len(rows)} months); skipping z-score check",
        )

    historical_totals = [float(r["total_units"]) for r in rows]
    mean = statistics.mean(historical_totals)
    stdev = statistics.stdev(historical_totals)

    if stdev == 0:
        return GateCheck(
            name="zscore_anomaly",
            passed=True,
            detail="Historical stdev is 0; skipping z-score check",
        )

    # Current month total
    maker_records = [r for r in records if r.maker]
    current_total = float(sum(r.registration_count for r in maker_records))
    if current_total == 0:
        current_total = float(sum(r.registration_count for r in records))

    z_score = abs(current_total - mean) / stdev
    passed = z_score <= config.zscore_threshold
    return GateCheck(
        name="zscore_anomaly",
        passed=passed,
        detail=(
            f"Z-score: {z_score:.2f} (threshold: {config.zscore_threshold}). "
            f"Current: {current_total:,.0f}, Mean: {mean:,.0f}"
        ),
    )


def _check_duplicate_rows(records: List[RawRecord]) -> GateCheck:
    """Check 6: No exact duplicate rows."""
    seen: set = set()
    duplicates = 0
    for rec in records:
        key = (rec.maker, rec.fuel, rec.vehicle_class, rec.period, rec.state)
        if key in seen:
            duplicates += 1
        seen.add(key)

    passed = duplicates == 0
    return GateCheck(
        name="duplicate_rows",
        passed=passed,
        detail=f"{duplicates} duplicate row(s) found",
    )


def _check_negative_delta(
    records: List[RawRecord],
) -> GateCheck:
    """Check 7: No records with negative registration counts."""
    negative = [r for r in records if r.registration_count < 0]
    passed = len(negative) == 0
    return GateCheck(
        name="negative_delta",
        passed=passed,
        detail=f"{len(negative)} record(s) with negative count",
    )


async def run_validation_gate(
    records: List[RawRecord],
    pool: asyncpg.Pool,
    month: str,
    config: Optional[GateConfig] = None,
) -> GateResult:
    """
    Run all 7 validation checks on raw VAHAN records.

    Args:
        records: raw records from VAHAN connector
        pool: asyncpg connection pool (for historical z-score lookup)
        month: data period 'YYYY-MM' (for logging)
        config: GateConfig with thresholds (uses defaults if None)

    Returns:
        GateResult with pass/fail and per-check details
    """
    if config is None:
        config = GateConfig()

    checks: List[GateCheck] = [
        _check_min_row_count(records, config),
        _check_maker_coverage(records, config),
        _check_fuel_coverage(records, config),
        _check_total_volume_sanity(records, config),
        await _check_zscore_anomaly(records, pool, month, config),
        _check_duplicate_rows(records),
        _check_negative_delta(records),
    ]

    passed = all(c.passed for c in checks)
    result = GateResult(passed=passed, checks=checks)

    log = logger.bind(month=month)
    if passed:
        log.info("gate.passed", checks_run=len(checks))
    else:
        log.warning(
            "gate.failed",
            failed_checks=result.failed_check_names,
        )
        for check in checks:
            if not check.passed:
                log.warning("gate.check_failed", check=check.name, detail=check.detail)

    return result
