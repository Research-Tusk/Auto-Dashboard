"""
AutoQuant ETL — ASP (Average Selling Price) Manager
=====================================================
Manages the fact_asp_master table which stores time-versioned ASP
assumptions used for revenue proxy calculations.

ASP records are SCD (Slowly Changing Dimension) Type 2:
- Each row has an effective_from / effective_to date range
- The current active row has effective_to = NULL
- Updating: close the old row (set effective_to = new effective_from - 1 day)
            then insert the new row

Source codes (by convention):
  EARNINGS_DISCLOSURE — from quarterly earnings calls / investor presentations
  INDUSTRY_ESTIMATE   — analyst / industry estimate
  MANUAL              — manually entered
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from typing import Optional

import asyncpg
import structlog

logger = structlog.get_logger(__name__)


@dataclass
class ASPUpdateResult:
    """Result of an ASP update operation."""
    old_asp: float      # Previous ASP in INR Lakhs (0.0 if no prior row)
    new_asp: float      # New ASP in INR Lakhs
    oem_id: int
    segment_id: int
    effective_from: date
    dry_run: bool = False


async def update_asp(
    pool: asyncpg.Pool,
    oem_name: str,
    segment_code: str,
    asp_inr_lakhs: float,
    effective_from: date,
    source: str = "EARNINGS_DISCLOSURE",
    notes: str = "",
    dry_run: bool = False,
) -> ASPUpdateResult:
    """
    Update the ASP assumption for an OEM × segment combination.

    Implements SCD Type 2 logic:
    1. Look up oem_id from dim_oem (by oem_name)
    2. Look up segment_id from dim_segment (by segment_code)
    3. Close the currently active ASP row (set effective_to = effective_from - 1)
    4. Insert a new row with the new ASP value

    Args:
        pool: asyncpg connection pool
        oem_name: OEM name as stored in dim_oem.oem_name
        segment_code: segment code (PV / CV / 2W)
        asp_inr_lakhs: new ASP in INR Lakhs
        effective_from: date from which the new ASP is effective
        source: source of the ASP data (default: EARNINGS_DISCLOSURE)
        notes: optional free-text notes
        dry_run: if True, skip DB writes and return the would-be result

    Returns:
        ASPUpdateResult with old and new ASP values

    Raises:
        ValueError: if OEM or segment is not found in dimension tables
    """
    async with pool.acquire() as conn:
        # Resolve oem_id
        oem_id: Optional[int] = await conn.fetchval(
            "SELECT oem_id FROM dim_oem WHERE LOWER(oem_name) = LOWER($1) LIMIT 1",
            oem_name,
        )
        if oem_id is None:
            raise ValueError(
                f"OEM '{oem_name}' not found in dim_oem. "
                "Check oem_name spelling (exact match, case-insensitive)."
            )

        # Resolve segment_id
        segment_id: Optional[int] = await conn.fetchval(
            "SELECT segment_id FROM dim_segment WHERE UPPER(segment_code) = UPPER($1) LIMIT 1",
            segment_code,
        )
        if segment_id is None:
            raise ValueError(
                f"Segment '{segment_code}' not found in dim_segment. "
                "Valid codes: PV, CV, 2W"
            )

        # Fetch the currently active ASP row
        current_row = await conn.fetchrow(
            """
            SELECT asp_id, asp_inr_lakhs
            FROM fact_asp_master
            WHERE oem_id = $1
              AND segment_id = $2
              AND effective_to IS NULL
            ORDER BY effective_from DESC
            LIMIT 1
            """,
            oem_id,
            segment_id,
        )

        old_asp = float(current_row["asp_inr_lakhs"]) if current_row else 0.0
        close_date = effective_from - timedelta(days=1)

        logger.info(
            "asp_manager.update",
            oem_name=oem_name,
            segment_code=segment_code,
            old_asp=old_asp,
            new_asp=asp_inr_lakhs,
            effective_from=effective_from.isoformat(),
            dry_run=dry_run,
        )

        if dry_run:
            return ASPUpdateResult(
                old_asp=old_asp,
                new_asp=asp_inr_lakhs,
                oem_id=oem_id,
                segment_id=segment_id,
                effective_from=effective_from,
                dry_run=True,
            )

        async with conn.transaction():
            # Close the current active row if one exists
            if current_row:
                await conn.execute(
                    """
                    UPDATE fact_asp_master
                    SET effective_to = $1
                    WHERE asp_id = $2
                    """,
                    close_date,
                    current_row["asp_id"],
                )

            # Insert new ASP row
            await conn.execute(
                """
                INSERT INTO fact_asp_master
                    (oem_id, segment_id, asp_inr_lakhs, effective_from, effective_to, source, notes)
                VALUES ($1, $2, $3, $4, NULL, $5, $6)
                """,
                oem_id,
                segment_id,
                asp_inr_lakhs,
                effective_from,
                source,
                notes or "",
            )

    logger.info(
        "asp_manager.updated",
        oem_id=oem_id,
        segment_id=segment_id,
        old_asp=old_asp,
        new_asp=asp_inr_lakhs,
    )

    return ASPUpdateResult(
        old_asp=old_asp,
        new_asp=asp_inr_lakhs,
        oem_id=oem_id,
        segment_id=segment_id,
        effective_from=effective_from,
        dry_run=False,
    )
