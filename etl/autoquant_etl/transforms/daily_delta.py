"""
AutoQuant ETL — Transform: Daily Delta Derivation
===================================================
VAHAN provides cumulative month-to-date (MTD) figures.
To get daily registrations, we compute:

    delta(t) = MTD(t) - MTD(t-1)

For the first day of a month: delta = MTD (no prior day)
For weekends/holidays: VAHAN may show the same MTD — delta could be 0.
Negative deltas can occur due to data corrections; they are set to 0.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Dict, List, Optional, Tuple

from autoquant_etl.transforms.normalize import NormalizedRecord


@dataclass
class DailyDeltaRecord:
    """A single day's registration delta for a specific dimension combination."""
    date_key: date
    oem_id: int
    segment_id: int
    fuel_id: int
    geo_id: int
    registration_count: int  # Always >= 0 (negative deltas clamped to 0)
    is_revision: bool = False


DimensionKey = Tuple[int, int, int, int]  # (oem_id, segment_id, fuel_id, geo_id)


def compute_daily_delta(
    normalized_records: List[NormalizedRecord],
    month_date: date,
    prior_mtd: Optional[Dict[DimensionKey, int]] = None,
) -> List[DailyDeltaRecord]:
    """
    Compute daily delta from MTD snapshot.

    For each (oem_id, segment_id, fuel_id, geo_id) combination:
        delta = current_mtd - prior_mtd

    If prior_mtd is None (i.e. this is the first extraction of the month),
    the delta equals the full MTD value.

    Args:
        normalized_records: Output of normalize_records()
        month_date: First day of the month (used as date_key)
        prior_mtd: Dict mapping dimension key → prior MTD count.
                   If None, delta = MTD (first day of month or no prior data).

    Returns:
        List of DailyDeltaRecord with non-negative counts
    """
    # Aggregate current MTD by dimension key
    current_mtd: Dict[DimensionKey, int] = {}
    for rec in normalized_records:
        if rec.oem_id is None or rec.segment_id is None or rec.fuel_id is None:
            continue  # Skip unmapped records
        key = (rec.oem_id, rec.segment_id, rec.fuel_id, rec.geo_id)
        current_mtd[key] = current_mtd.get(key, 0) + rec.registration_count

    if prior_mtd is None:
        prior_mtd = {}

    # Compute deltas
    delta_records: List[DailyDeltaRecord] = []
    all_keys = set(current_mtd.keys()) | set(prior_mtd.keys())

    for key in all_keys:
        oem_id, segment_id, fuel_id, geo_id = key
        current = current_mtd.get(key, 0)
        prior = prior_mtd.get(key, 0)
        delta = current - prior

        # Clamp negative deltas to 0 (data correction artefact)
        is_revision = delta < 0
        delta = max(0, delta)

        delta_records.append(DailyDeltaRecord(
            date_key=month_date,
            oem_id=oem_id,
            segment_id=segment_id,
            fuel_id=fuel_id,
            geo_id=geo_id,
            registration_count=delta,
            is_revision=is_revision,
        ))

    return delta_records
