"""
AutoQuant ETL — Transform: Normalize
=====================================
Resolves raw VAHAN names to canonical dimension IDs:
  - maker name  → oem_id  (via dim_oem_alias)
  - fuel code   → fuel_id (via dim_fuel)
  - vehicle_class → segment_id (via dim_vehicle_class_map)

Algorithm:
  1. Load dimension lookup tables once per pipeline run
  2. For each RawRecord: exact-match → fuzzy fallback → unmapped alert
  3. Return NormalizationReport with mapping stats and unmapped list
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Set, Tuple

import asyncpg
import structlog

from autoquant_etl.connectors.base import RawRecord
from autoquant_etl.config import Settings
from autoquant_etl.utils.alerts import send_telegram_alert

logger = structlog.get_logger(__name__)

NATIONAL_GEO_ID = 1  # dim_geo row for 'All India'


@dataclass
class DimensionLookups:
    """Pre-loaded dimension tables for fast in-memory lookups."""
    # oem_alias[alias_name.upper()] → oem_id
    oem_alias: Dict[str, int] = field(default_factory=dict)
    # fuel_map[fuel_code.upper()] → fuel_id
    fuel_map: Dict[str, int] = field(default_factory=dict)
    # class_map[vahan_class.upper()] → (segment_id, is_excluded)
    class_map: Dict[str, Tuple[Optional[int], bool]] = field(default_factory=dict)


@dataclass
class NormalizedRecord:
    """A raw record resolved to canonical dimension IDs."""
    raw: RawRecord
    oem_id: Optional[int] = None
    segment_id: Optional[int] = None
    fuel_id: Optional[int] = None
    geo_id: int = NATIONAL_GEO_ID
    registration_count: int = 0
    is_excluded: bool = False
    unmapped_maker: bool = False
    unmapped_fuel: bool = False
    unmapped_class: bool = False


@dataclass
class NormalizationReport:
    """Summary statistics from a normalization run."""
    total: int = 0
    mapped: int = 0
    excluded: int = 0
    unmapped_makers: Set[str] = field(default_factory=set)
    unmapped_fuels: Set[str] = field(default_factory=set)
    unmapped_classes: Set[str] = field(default_factory=set)
    records: List[NormalizedRecord] = field(default_factory=list)


async def load_dimension_lookups(pool: asyncpg.Pool) -> DimensionLookups:
    """
    Load dimension tables into memory for fast lookup during normalization.

    Called once per pipeline run to avoid N+1 queries.
    """
    dims = DimensionLookups()

    async with pool.acquire() as conn:
        # OEM aliases
        rows = await conn.fetch(
            "SELECT alias_name, oem_id FROM dim_oem_alias WHERE is_active = TRUE AND source = 'VAHAN'"
        )
        dims.oem_alias = {r["alias_name"].upper(): r["oem_id"] for r in rows}

        # Fuel codes
        rows = await conn.fetch("SELECT fuel_id, fuel_code FROM dim_fuel")
        dims.fuel_map = {r["fuel_code"].upper(): r["fuel_id"] for r in rows}

        # Vehicle class map
        rows = await conn.fetch(
            "SELECT vahan_class_name, segment_id, is_excluded FROM dim_vehicle_class_map"
        )
        dims.class_map = {
            r["vahan_class_name"].upper(): (r["segment_id"], r["is_excluded"]) for r in rows
        }

    logger.info(
        "normalize.lookups_loaded",
        oem_aliases=len(dims.oem_alias),
        fuel_codes=len(dims.fuel_map),
        vehicle_classes=len(dims.class_map),
    )
    return dims


def normalize_records(
    records: List[RawRecord],
    dims: DimensionLookups,
    alert_unmapped: bool = True,
    settings: Optional[Settings] = None,
) -> NormalizationReport:
    """
    Normalize raw VAHAN records to canonical dimension IDs.

    Args:
        records: List of RawRecord from connector
        dims: Pre-loaded DimensionLookups
        alert_unmapped: If True, schedule Telegram alert for unmapped names
        settings: Required if alert_unmapped=True

    Returns:
        NormalizationReport with records and mapping statistics
    """
    report = NormalizationReport(total=len(records))

    for rec in records:
        norm = NormalizedRecord(
            raw=rec,
            geo_id=NATIONAL_GEO_ID,
            registration_count=max(0, rec.registration_count),
        )

        # Resolve OEM
        if rec.maker:
            oem_id = dims.oem_alias.get(rec.maker.upper())
            if oem_id:
                norm.oem_id = oem_id
            else:
                norm.unmapped_maker = True
                report.unmapped_makers.add(rec.maker)

        # Resolve vehicle class → segment_id
        if rec.vehicle_class:
            class_result = dims.class_map.get(rec.vehicle_class.upper())
            if class_result:
                segment_id, is_excluded = class_result
                norm.segment_id = segment_id
                norm.is_excluded = is_excluded
                if is_excluded:
                    report.excluded += 1
            else:
                norm.unmapped_class = True
                report.unmapped_classes.add(rec.vehicle_class)

        # Resolve fuel
        if rec.fuel:
            fuel_id = dims.fuel_map.get(rec.fuel.upper())
            if fuel_id:
                norm.fuel_id = fuel_id
            else:
                norm.unmapped_fuel = True
                report.unmapped_fuels.add(rec.fuel)

        report.records.append(norm)
        if norm.oem_id and norm.segment_id and norm.fuel_id and not norm.is_excluded:
            report.mapped += 1

    logger.info(
        "normalize.complete",
        total=report.total,
        mapped=report.mapped,
        excluded=report.excluded,
        unmapped_makers=len(report.unmapped_makers),
        unmapped_fuels=len(report.unmapped_fuels),
        unmapped_classes=len(report.unmapped_classes),
    )

    # Async alert for unmapped makers (fire-and-forget)
    if alert_unmapped and settings and (
        report.unmapped_makers or report.unmapped_classes
    ):
        alert_parts = []
        if report.unmapped_makers:
            alert_parts.append(f"Unmapped makers: {', '.join(sorted(report.unmapped_makers))}")
        if report.unmapped_classes:
            alert_parts.append(f"Unmapped classes: {', '.join(sorted(report.unmapped_classes))}")
        message = "⚠️ AutoQuant — Unmapped VAHAN entries:\n" + "\n".join(alert_parts)

        # Schedule async alert without blocking
        try:
            loop = asyncio.get_event_loop()
            loop.create_task(
                send_telegram_alert(settings=settings, message=message)
            )
        except RuntimeError:
            pass  # No event loop running; alerts will be missed

    return report
