"""
AutoQuant ETL — Gold Layer: Revenue Estimation
================================================
Computes quarterly demand-based revenue proxy:

  revenue_retail_cr = units_retail × asp_inr_lakhs / 100

Where:
  units_retail    = sum of registrations in fact_monthly_registrations
                    for the quarter date range
  asp_inr_lakhs   = current active ASP from fact_asp_master
  / 100           = converts Lakhs to Crores

Results are upserted into est_quarterly_revenue.

Data completeness is computed as:
  (OEMs with both registration data AND ASP) / (total OEMs with data)
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional

import asyncpg
import structlog

from autoquant_etl.config import Settings
from autoquant_etl.utils.fy_calendar import (
    fy_quarter_date_range,
    current_fy_quarter,
    date_to_fy_quarter,
)

logger = structlog.get_logger(__name__)


@dataclass
class RevenueEstRow:
    """A single OEM × segment revenue estimate row."""
    oem_name: str
    segment_code: str
    units_retail: int
    asp_used: float           # INR Lakhs
    revenue_retail_cr: float  # INR Crores


@dataclass
class RevenueResult:
    """Result of a quarterly revenue estimation run."""
    quarter: str
    oem_count: int
    rows: List[RevenueEstRow] = field(default_factory=list)
    data_completeness: float = 0.0
    dry_run: bool = False


async def run_revenue_estimation(
    pool: asyncpg.Pool,
    settings: Settings,
    quarter: Optional[str] = None,
    dry_run: bool = False,
    verbose: bool = False,
) -> RevenueResult:
    """
    Compute quarterly demand-based revenue proxy and upsert into est_quarterly_revenue.

    Args:
        pool: asyncpg connection pool
        settings: application settings
        quarter: FY quarter string e.g. "Q3FY26". Defaults to current quarter.
        dry_run: if True, compute but skip DB writes
        verbose: enable verbose logging

    Returns:
        RevenueResult with computed rows and metadata
    """
    from datetime import date as date_type

    # Resolve quarter
    if not quarter:
        quarter = current_fy_quarter()

    start_date, end_date = fy_quarter_date_range(quarter)

    log = logger.bind(quarter=quarter, start=start_date.isoformat(), end=end_date.isoformat())
    log.info("gold.revenue_estimation_start")

    async with pool.acquire() as conn:
        # Query registration units aggregated by OEM × segment for the quarter
        reg_rows = await conn.fetch(
            """
            SELECT
                o.oem_id,
                o.oem_name,
                s.segment_id,
                s.segment_code,
                SUM(mr.units) AS units_retail
            FROM fact_monthly_registrations mr
            JOIN dim_oem       o ON o.oem_id      = mr.oem_id
            JOIN dim_segment   s ON s.segment_id  = mr.segment_id
            WHERE mr.month_key >= $1
              AND mr.month_key <= $2
            GROUP BY o.oem_id, o.oem_name, s.segment_id, s.segment_code
            HAVING SUM(mr.units) > 0
            ORDER BY o.oem_name, s.segment_code
            """,
            start_date,
            end_date,
        )

        if not reg_rows:
            log.warning("gold.no_registration_data")
            return RevenueResult(quarter=quarter, oem_count=0, dry_run=dry_run)

        # Query active ASPs for each OEM × segment combination
        # Using a lateral join approach: get the most recent active ASP
        asp_rows = await conn.fetch(
            """
            SELECT
                a.oem_id,
                a.segment_id,
                a.asp_inr_lakhs
            FROM fact_asp_master a
            WHERE a.effective_to IS NULL
               OR a.effective_to >= $1
            """,
            start_date,
        )

        # Build ASP lookup: (oem_id, segment_id) → asp_inr_lakhs
        asp_map = {
            (row["oem_id"], row["segment_id"]): float(row["asp_inr_lakhs"])
            for row in asp_rows
        }

    revenue_rows: List[RevenueEstRow] = []
    oem_ids_with_data = set()
    oem_ids_with_asp = set()

    rows_to_insert = []

    for reg in reg_rows:
        oem_id = reg["oem_id"]
        segment_id = reg["segment_id"]
        units = int(reg["units_retail"])
        oem_ids_with_data.add(oem_id)

        asp = asp_map.get((oem_id, segment_id))
        if asp is None:
            if verbose:
                log.debug(
                    "gold.no_asp",
                    oem_name=reg["oem_name"],
                    segment_code=reg["segment_code"],
                )
            continue

        oem_ids_with_asp.add(oem_id)
        revenue_cr = round(units * asp / 100, 2)

        revenue_rows.append(
            RevenueEstRow(
                oem_name=reg["oem_name"],
                segment_code=reg["segment_code"],
                units_retail=units,
                asp_used=asp,
                revenue_retail_cr=revenue_cr,
            )
        )
        rows_to_insert.append(
            (quarter, oem_id, segment_id, units, asp, revenue_cr)
        )

    # Data completeness
    completeness = (
        len(oem_ids_with_asp) / len(oem_ids_with_data)
        if oem_ids_with_data
        else 0.0
    )

    log.info(
        "gold.revenue_computed",
        rows=len(revenue_rows),
        oem_count=len(oem_ids_with_asp),
        completeness=round(completeness, 3),
        dry_run=dry_run,
    )

    if not dry_run and rows_to_insert:
        async with pool.acquire() as conn:
            await conn.executemany(
                """
                INSERT INTO est_quarterly_revenue
                    (fy_quarter, oem_id, segment_id, units_retail, asp_used,
                     revenue_retail_cr, data_completeness, generated_at)
                VALUES ($1, $2, $3, $4, $5, $6, $7, NOW())
                ON CONFLICT (fy_quarter, oem_id, segment_id)
                DO UPDATE SET
                    units_retail       = EXCLUDED.units_retail,
                    asp_used           = EXCLUDED.asp_used,
                    revenue_retail_cr  = EXCLUDED.revenue_retail_cr,
                    data_completeness  = EXCLUDED.data_completeness,
                    generated_at       = NOW()
                """,
                [
                    (fy_q, oem_id, seg_id, units, asp, rev_cr, round(completeness, 4))
                    for fy_q, oem_id, seg_id, units, asp, rev_cr in rows_to_insert
                ],
            )
        log.info("gold.revenue_upserted", count=len(rows_to_insert))

    return RevenueResult(
        quarter=quarter,
        oem_count=len(oem_ids_with_asp),
        rows=revenue_rows,
        data_completeness=completeness,
        dry_run=dry_run,
    )
