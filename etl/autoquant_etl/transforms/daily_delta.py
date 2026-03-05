"""
AutoQuant ETL — Transform: Daily Delta
========================================
Converts MTD (Month-to-Date) cumulative registration counts from VAHAN
into true daily incremental counts.

VAHAN reports MTD totals as of the capture date, not single-day counts.
To get a daily figure we subtract the previous day's MTD:

  daily(day N) = MTD(day N) - MTD(day N-1)
  daily(day 1) = MTD(day 1)   # no previous day to subtract

Since in practice we only have one snapshot per extraction run (not one per
day), this module treats the normalized records as representing the latest
MTD snapshot and marks them with the extraction date as the date_key.

For historical backfill, records are loaded once per month-end and the
daily split is synthetic (all assigned to the last day of the month).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from typing import Dict, List, Optional, Tuple

import structlog

from autoquant_etl.transforms.normalize import NormalizedRecord

logger = structlog.get_logger(__name__)


@dataclass
class DailyDeltaRecord:
    """A single daily delta registration record ready for DB insertion."""
    date_key: date
    oem_id: Optional[int]
    segment_id: Optional[int]
    fuel_id: Optional[int]
    geo_id: int
    registration_count: int
    is_revision: bool = False


# Type alias for the dimension key tuple used to group records
_DimKey = Tuple[Optional[int], Optional[int], Optional[int], int]


def compute_daily_delta(
    normalized_records: List[NormalizedRecord],
    month_date: date,
    prior_mtd: Optional[Dict[_DimKey, int]] = None,
) -> List[DailyDeltaRecord]:
    """
    Compute daily incremental registration counts from MTD snapshots.

    The VAHAN dashboard provides cumulative MTD (Month-to-Date) counts.
    To derive the *incremental* count for a single day:

        daily(today) = MTD(today) - MTD(yesterday)

    For the first day of the month, daily == MTD (no prior day).

    Args:
        normalized_records: output of normalize_records() — NormalizationReport.records
        month_date: first day of the month being processed (e.g. date(2026,3,1))
        prior_mtd: optional dict mapping dimension key → previous MTD count.
                   If provided, used for delta computation.
                   Keys are (oem_id, segment_id, fuel_id, geo_id).

    Returns:
        List of DailyDeltaRecord with date_key = the extraction date
        (last day of available data, approximated as today for live runs
        or month_date for backfill).
    """
    # Determine the date_key: we use today's date for live runs.
    # The caller can pass month_date to control which date gets the records.
    # For a live daily run: use today's date (or yesterday, same as extract_date).
    # We use month_date as a baseline and the caller in orchestrator passes
    # extract_date.replace(day=1) — so here we use today's date for the key.
    today = date.today()

    # If month_date is the first of the month and no prior MTD, all is day-1 logic
    is_first_of_month = (today.month == month_date.month and today.day == 1) or (
        prior_mtd is None
    )

    # Group incoming normalized records by dimension key, summing counts
    current_mtd: Dict[_DimKey, int] = {}
    for rec in normalized_records:
        if rec.is_excluded:
            continue
        # Only include records that have at minimum oem_id or segment_id
        key: _DimKey = (rec.oem_id, rec.segment_id, rec.fuel_id, rec.geo_id)
        current_mtd[key] = current_mtd.get(key, 0) + rec.registration_count

    delta_records: List[DailyDeltaRecord] = []
    is_revision = prior_mtd is not None

    for key, mtd_count in current_mtd.items():
        oem_id, segment_id, fuel_id, geo_id = key

        if is_first_of_month or prior_mtd is None:
            # Day 1 of month OR no prior data available: daily = MTD
            daily_count = mtd_count
        else:
            prior_count = prior_mtd.get(key, 0)
            daily_count = max(0, mtd_count - prior_count)

        delta_records.append(
            DailyDeltaRecord(
                date_key=today,
                oem_id=oem_id,
                segment_id=segment_id,
                fuel_id=fuel_id,
                geo_id=geo_id,
                registration_count=daily_count,
                is_revision=is_revision,
            )
        )

    total_daily = sum(r.registration_count for r in delta_records)
    logger.info(
        "daily_delta.computed",
        records=len(delta_records),
        total_registrations=total_daily,
        month=month_date.isoformat(),
        is_revision=is_revision,
    )

    return delta_records
