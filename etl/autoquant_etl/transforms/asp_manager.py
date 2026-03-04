"""
AutoQuant ETL — ASP Manager
=============================
CRUD operations for Average Selling Price (ASP) assumptions
stored in fact_asp_master.

PROMINENT DISCLAIMER (must appear on ALL financial output):
  Revenue figures are DEMAND-BASED PROXIES:
  retail_registrations × analyst_ASP_assumption ≠ reported accounting revenue.
  For investment decisions, always use the OEM's official quarterly results.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import List, Optional

import asyncpg
import structlog

logger = structlog.get_logger(__name__)


@dataclass
class ASPRecord:
    asp_id: int
    oem_name: str
    segment_code: str
    fuel_id: int
    effective_from: date
    effective_to: Optional[date]
    asp_inr_lakhs: float
    source: str
    notes: Optional[str]


@dataclass
class ASPUpdateResult:
    old_asp: float
    new_asp: float
    oem_name: str
    segment_code: str
    effective_from: date


async def get_current_asp(
    pool: asyncpg.Pool,
    oem_name: str,
    segment_code: str,
    fuel_id: int = 0,
    as_of: Optional[date] = None,
) -> Optional[ASPRecord]:
    """
    Get the current (most recent effective) ASP for an OEM + segment.

    Args:
        pool: asyncpg connection pool
        oem_name: canonical OEM name from dim_oem
        segment_code: 'PV', 'CV', or '2W'
        fuel_id: fuel dimension ID (0 = all fuels)
        as_of: date for which ASP is needed (defaults to today)

    Returns:
        ASPRecord or None if not found
    """
    as_of = as_of or date.today()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT
                fam.asp_id,
                do2.oem_name,
                ds.segment_code,
                fam.fuel_id,
                fam.effective_from,
                fam.effective_to,
                fam.asp_inr_lakhs,
                fam.source,
                fam.notes
            FROM fact_asp_master fam
            JOIN dim_oem do2 ON do2.oem_id = fam.oem_id
            JOIN dim_segment ds ON ds.segment_id = fam.segment_id AND ds.sub_segment IS NULL
            WHERE do2.oem_name = $1
              AND ds.segment_code = $2
              AND fam.fuel_id = $3
              AND fam.effective_from <= $4
              AND (fam.effective_to IS NULL OR fam.effective_to >= $4)
            ORDER BY fam.effective_from DESC
            LIMIT 1
            """,
            oem_name, segment_code, fuel_id, as_of,
        )
    if not row:
        return None
    return ASPRecord(
        asp_id=row["asp_id"],
        oem_name=row["oem_name"],
        segment_code=row["segment_code"],
        fuel_id=row["fuel_id"],
        effective_from=row["effective_from"],
        effective_to=row["effective_to"],
        asp_inr_lakhs=float(row["asp_inr_lakhs"]),
        source=row["source"],
        notes=row["notes"],
    )


async def update_asp(
    pool: asyncpg.Pool,
    oem_name: str,
    segment_code: str,
    asp_inr_lakhs: float,
    effective_from: Optional[date] = None,
    fuel_id: int = 0,
    source: str = "ANALYST_ESTIMATE",
    notes: str = "",
    dry_run: bool = False,
) -> ASPUpdateResult:
    """
    Insert a new ASP record and close out the previous one.

    This implements SCD Type 2 behaviour:
      1. Set effective_to = effective_from - 1 day on current record
      2. Insert new record with new asp_inr_lakhs and effective_from

    Args:
        pool: asyncpg connection pool
        oem_name: canonical OEM name
        segment_code: 'PV', 'CV', or '2W'
        asp_inr_lakhs: new ASP value in INR Lakhs
        effective_from: date from which new ASP applies (default: today)
        fuel_id: fuel dimension ID (0 = all fuels)
        source: source of the ASP update
        notes: optional notes
        dry_run: if True, validate only, don't write

    Returns:
        ASPUpdateResult with old and new ASP values
    """
    effective_from = effective_from or date.today()
    log = logger.bind(oem=oem_name, segment=segment_code, asp=asp_inr_lakhs)

    # Get current ASP
    current = await get_current_asp(pool, oem_name, segment_code, fuel_id, as_of=effective_from)
    old_asp = current.asp_inr_lakhs if current else 0.0

    if dry_run:
        log.info("asp_manager.dry_run", old_asp=old_asp, new_asp=asp_inr_lakhs)
        return ASPUpdateResult(
            old_asp=old_asp,
            new_asp=asp_inr_lakhs,
            oem_name=oem_name,
            segment_code=segment_code,
            effective_from=effective_from,
        )

    async with pool.acquire() as conn:
        async with conn.transaction():
            # Resolve OEM and segment IDs
            oem_id = await conn.fetchval(
                "SELECT oem_id FROM dim_oem WHERE oem_name = $1", oem_name
            )
            if not oem_id:
                raise ValueError(f"OEM not found: {oem_name}")

            segment_id = await conn.fetchval(
                "SELECT segment_id FROM dim_segment WHERE segment_code = $1 AND sub_segment IS NULL",
                segment_code,
            )
            if not segment_id:
                raise ValueError(f"Segment not found: {segment_code}")

            # Close out current record if exists
            if current:
                await conn.execute(
                    """
                    UPDATE fact_asp_master
                    SET effective_to = $1 - INTERVAL '1 day'
                    WHERE asp_id = $2
                    """,
                    effective_from,
                    current.asp_id,
                )

            # Insert new ASP record
            await conn.execute(
                """
                INSERT INTO fact_asp_master
                    (oem_id, segment_id, fuel_id, effective_from, asp_inr_lakhs, source, notes)
                VALUES ($1, $2, $3, $4, $5, $6, $7)
                """,
                oem_id, segment_id, fuel_id, effective_from,
                asp_inr_lakhs, source, notes,
            )

    log.info("asp_manager.updated", old_asp=old_asp, new_asp=asp_inr_lakhs)
    return ASPUpdateResult(
        old_asp=old_asp,
        new_asp=asp_inr_lakhs,
        oem_name=oem_name,
        segment_code=segment_code,
        effective_from=effective_from,
    )


async def list_asp_assumptions(
    pool: asyncpg.Pool,
    as_of: Optional[date] = None,
    segment_code: Optional[str] = None,
) -> List[ASPRecord]:
    """
    List all current ASP assumptions.

    Args:
        pool: asyncpg connection pool
        as_of: date for which ASPs are needed (defaults to today)
        segment_code: optional filter by segment

    Returns:
        List of ASPRecord
    """
    as_of = as_of or date.today()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT
                fam.asp_id,
                do2.oem_name,
                ds.segment_code,
                fam.fuel_id,
                fam.effective_from,
                fam.effective_to,
                fam.asp_inr_lakhs,
                fam.source,
                fam.notes
            FROM fact_asp_master fam
            JOIN dim_oem do2 ON do2.oem_id = fam.oem_id
            JOIN dim_segment ds ON ds.segment_id = fam.segment_id AND ds.sub_segment IS NULL
            WHERE fam.effective_from <= $1
              AND (fam.effective_to IS NULL OR fam.effective_to >= $1)
              AND ($2::VARCHAR IS NULL OR ds.segment_code = $2)
            ORDER BY ds.segment_code, do2.oem_name
            """,
            as_of, segment_code,
        )
    return [
        ASPRecord(
            asp_id=r["asp_id"],
            oem_name=r["oem_name"],
            segment_code=r["segment_code"],
            fuel_id=r["fuel_id"],
            effective_from=r["effective_from"],
            effective_to=r["effective_to"],
            asp_inr_lakhs=float(r["asp_inr_lakhs"]),
            source=r["source"],
            notes=r["notes"],
        )
        for r in rows
    ]
