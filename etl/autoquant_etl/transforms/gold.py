"""
AutoQuant ETL — Gold Layer: Revenue Proxy Computation
======================================================
Computes estimated quarterly revenue from:
  registration_volume × ASP_assumption = implied_revenue_proxy

PROMINENT DISCLAIMER (reproduced on all output):
  These are DEMAND-BASED PROXIES, not accounting revenue.
  Retail registrations × analyst ASP assumption ≠ reported earnings.
  Do NOT use for investment decisions without official OEM results.

Data flow:
  fact_monthly_registrations (Silver)
  + fact_asp_master (Gold)
  → est_quarterly_revenue (Gold)
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from typing import List, Optional

import asyncpg
import structlog

from autoquant_etl.config import Settings

logger = structlog.get_logger(__name__)

DISCLAIMER = (
    "DISCLAIMER: Revenue figures are demand-based proxies "
    "(registrations × ASP assumption). NOT accounting revenue. "
    "Do NOT use for investment decisions without official OEM results."
)


@dataclass
class RevenueRow:
    fy_quarter: str
    oem_id: int
    oem_name: str
    segment_id: int
    segment_code: str
    units_retail: int
    asp_used: float
    revenue_retail_cr: float
    data_completeness: float


@dataclass
class RevenueEstimationResult:
    quarter: str
    oem_count: int
    rows: List[RevenueRow] = field(default_factory=list)
    disclaimer: str = DISCLAIMER


async def run_revenue_estimation(
    pool: asyncpg.Pool,
    settings: Settings,
    quarter: Optional[str] = None,
    dry_run: bool = False,
    verbose: bool = False,
) -> RevenueEstimationResult:
    """
    Compute quarterly revenue proxy for all in-scope OEMs.

    Args:
        pool: asyncpg connection pool
        settings: application settings
        quarter: FY quarter e.g. 'Q3FY26'. Defaults to current quarter.
        dry_run: if True, compute but don't write to est_quarterly_revenue
        verbose: enable verbose logging

    Returns:
        RevenueEstimationResult with per-OEM revenue rows
    """
    log = logger.bind(quarter=quarter, dry_run=dry_run)
    log.info("gold.revenue_estimation_start")

    async with pool.acquire() as conn:
        # Resolve quarter
        if not quarter:
            row = await conn.fetchrow(
                "SELECT fy_quarter FROM dim_date WHERE date_key = CURRENT_DATE"
            )
            quarter = row["fy_quarter"] if row else None
            if not quarter:
                raise ValueError("Cannot determine current FY quarter from dim_date")

        log = log.bind(quarter=quarter)

        # Get all months in the quarter
        month_rows = await conn.fetch(
            "SELECT DISTINCT date_key FROM dim_date WHERE fy_quarter = $1 "
            "AND calendar_month = EXTRACT(MONTH FROM date_key) "
            "ORDER BY date_key",
            quarter,
        )
        quarter_months = [r["date_key"] for r in month_rows]

        if not quarter_months:
            raise ValueError(f"No months found in dim_date for quarter {quarter}")

        # Compute data completeness
        # (number of months with data / expected months in quarter)
        expected_months = 3
        months_with_data = await conn.fetchval(
            """
            SELECT COUNT(DISTINCT month_key)
            FROM fact_monthly_registrations
            WHERE month_key = ANY($1)
            """,
            quarter_months,
        )
        completeness = float(months_with_data or 0) / expected_months
        log.info("gold.quarter_months", months_with_data=months_with_data, completeness=completeness)

        # Get quarterly units by OEM + segment
        rows = await conn.fetch(
            """
            SELECT
                do2.oem_id,
                do2.oem_name,
                ds.segment_id,
                ds.segment_code,
                SUM(fmr.units) AS units_retail
            FROM fact_monthly_registrations fmr
            JOIN dim_oem do2 ON do2.oem_id = fmr.oem_id
            JOIN dim_segment ds ON ds.segment_id = fmr.segment_id AND ds.sub_segment IS NULL
            WHERE fmr.month_key = ANY($1)
              AND do2.is_in_scope = TRUE
            GROUP BY do2.oem_id, do2.oem_name, ds.segment_id, ds.segment_code
            ORDER BY ds.segment_code, do2.oem_name
            """,
            quarter_months,
        )

        result_rows: List[RevenueRow] = []

        for row in rows:
            # Get ASP assumption for this OEM + segment
            asp_row = await conn.fetchrow(
                """
                SELECT asp_inr_lakhs
                FROM fact_asp_master
                WHERE oem_id = $1
                  AND segment_id = $2
                  AND fuel_id = 0
                  AND effective_from <= CURRENT_DATE
                  AND (effective_to IS NULL OR effective_to >= CURRENT_DATE)
                ORDER BY effective_from DESC
                LIMIT 1
                """,
                row["oem_id"], row["segment_id"],
            )

            if not asp_row:
                log.warning("gold.no_asp", oem=row["oem_name"], segment=row["segment_code"])
                continue

            asp_lakhs = float(asp_row["asp_inr_lakhs"])
            units = int(row["units_retail"] or 0)

            # Revenue in Crore = (units * ASP_lakhs) / 100
            # (1 Crore = 100 Lakhs)
            revenue_cr = round((units * asp_lakhs) / 100, 2)

            result_rows.append(RevenueRow(
                fy_quarter=quarter,
                oem_id=row["oem_id"],
                oem_name=row["oem_name"],
                segment_id=row["segment_id"],
                segment_code=row["segment_code"],
                units_retail=units,
                asp_used=asp_lakhs,
                revenue_retail_cr=revenue_cr,
                data_completeness=completeness,
            ))

        if not dry_run and result_rows:
            # Upsert into est_quarterly_revenue
            async with pool.acquire() as write_conn:
                async with write_conn.transaction():
                    for rr in result_rows:
                        await write_conn.execute(
                            """
                            INSERT INTO est_quarterly_revenue
                                (fy_quarter, oem_id, segment_id, units_retail,
                                 asp_used, revenue_retail_cr, data_completeness)
                            VALUES ($1, $2, $3, $4, $5, $6, $7)
                            ON CONFLICT (fy_quarter, oem_id, segment_id) DO UPDATE SET
                                units_retail       = EXCLUDED.units_retail,
                                asp_used           = EXCLUDED.asp_used,
                                revenue_retail_cr  = EXCLUDED.revenue_retail_cr,
                                data_completeness  = EXCLUDED.data_completeness,
                                generated_at       = NOW()
                            """,
                            rr.fy_quarter, rr.oem_id, rr.segment_id, rr.units_retail,
                            rr.asp_used, rr.revenue_retail_cr, rr.data_completeness,
                        )
            log.info("gold.upserted", count=len(result_rows))

        log.info("gold.revenue_estimation_done", oem_count=len(result_rows))
        return RevenueEstimationResult(
            quarter=quarter,
            oem_count=len(result_rows),
            rows=result_rows,
        )
