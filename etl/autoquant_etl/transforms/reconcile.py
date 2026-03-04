"""
AutoQuant ETL — Monthly Reconciliation Engine
===============================================
Compares VAHAN monthly aggregated registrations against FADA monthly
press release data to validate data quality and flag anomalies.

Reconciliation process:
  1. Sum VAHAN data for the target month by OEM + segment
  2. Match against FADA OEM totals for same month
  3. Compute delta and delta% for each OEM
  4. Flag OEMs where delta% exceeds threshold
  5. Insert reconciliation results into raw_fada_monthly
  6. Send Telegram alert if too many OEMs exceed threshold

Note: VAHAN and FADA can legitimately differ by 3-8% due to:
  - Timing: FADA is wholesale, VAHAN is retail registration
  - Scope: FADA may exclude certain vehicle classes
  - Methodology: FADA reports member OEMs only
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from typing import Dict, List, Optional

import asyncpg
import structlog

from autoquant_etl.config import Settings
from autoquant_etl.connectors.fada import FadaConnector
from autoquant_etl.connectors.base import ExtractParams
from autoquant_etl.utils.alerts import send_telegram_alert

logger = structlog.get_logger(__name__)

# Delta threshold: OEMs with |delta%| > this are flagged
DEFAULT_DELTA_THRESHOLD = 15.0  # percent
# Minimum FADA volume to include in reconciliation
MIN_FADA_VOLUME = 100


@dataclass
class OEMReconciliation:
    oem_name: str
    segment: str
    vahan_volume: int
    fada_volume: int
    delta: int
    delta_pct: float
    flagged: bool


@dataclass
class ReconciliationResult:
    report_month: date
    passed: bool
    total_delta_pct: float
    oem_reconciliations: List[OEMReconciliation] = field(default_factory=list)
    issues: List[str] = field(default_factory=list)


async def run_reconciliation(
    pool: asyncpg.Pool,
    settings: Settings,
    report_month: date,
    pdf_path: Optional[str] = None,
    pdf_url: Optional[str] = None,
    dry_run: bool = False,
    verbose: bool = False,
    delta_threshold: float = DEFAULT_DELTA_THRESHOLD,
) -> ReconciliationResult:
    """
    Run monthly VAHAN vs FADA reconciliation.

    Args:
        pool: asyncpg connection pool
        settings: application settings
        report_month: first day of the month to reconcile
        pdf_path: local path to FADA PDF (optional)
        pdf_url: URL of FADA PDF (optional)
        dry_run: if True, run reconciliation but don't write to DB
        verbose: enable verbose logging
        delta_threshold: % threshold for flagging OEMs

    Returns:
        ReconciliationResult with per-OEM comparison
    """
    log = logger.bind(
        report_month=report_month.isoformat(),
        dry_run=dry_run,
    )
    log.info("reconcile.start")

    # Step 1: Get VAHAN monthly totals
    async with pool.acquire() as conn:
        vahan_rows = await conn.fetch(
            """
            SELECT
                do2.oem_name,
                ds.segment_code,
                SUM(fmr.units) AS vahan_volume
            FROM fact_monthly_registrations fmr
            JOIN dim_oem do2 ON do2.oem_id = fmr.oem_id
            JOIN dim_segment ds ON ds.segment_id = fmr.segment_id AND ds.sub_segment IS NULL
            WHERE fmr.month_key = $1
              AND do2.is_in_scope = TRUE
            GROUP BY do2.oem_name, ds.segment_code
            ORDER BY ds.segment_code, do2.oem_name
            """,
            report_month,
        )

    vahan_volumes: Dict[tuple, int] = {
        (r["oem_name"], r["segment_code"]): int(r["vahan_volume"] or 0)
        for r in vahan_rows
    }

    log.info("reconcile.vahan_loaded", oem_count=len(vahan_volumes))

    # Step 2: Extract FADA data
    if not pdf_path and not pdf_url:
        log.warning("reconcile.no_fada_source", msg="No FADA PDF provided; using DB only")
        # Try to use existing raw_fada_monthly data
        async with pool.acquire() as conn:
            fada_rows = await conn.fetch(
                """
                SELECT
                    rfm.oem_name,
                    rfm.category AS segment,
                    SUM(rfm.volume_current) AS fada_volume
                FROM raw_fada_monthly rfm
                WHERE rfm.report_month = $1
                GROUP BY rfm.oem_name, rfm.category
                """,
                report_month,
            )
        fada_volumes: Dict[str, int] = {
            r["oem_name"]: int(r["fada_volume"] or 0) for r in fada_rows
        }
    else:
        # Extract from PDF
        async with FadaConnector(settings) as fc:
            params = ExtractParams(
                period=report_month.strftime("%Y-%m"),
                extra_params={"pdf_path": pdf_path, "pdf_url": pdf_url},
            )
            fada_result = await fc.extract(params)

        fada_volumes = {}
        for rec in fada_result.records:
            if rec.registration_count >= MIN_FADA_VOLUME:
                key = rec.maker or "UNKNOWN"
                fada_volumes[key] = fada_volumes.get(key, 0) + rec.registration_count

        # Persist FADA data if not dry_run
        if not dry_run:
            async with pool.acquire() as conn:
                # Get or create run_id
                run_id = await conn.fetchval(
                    """
                    INSERT INTO raw_extraction_log (source, status, records_extracted)
                    VALUES ('FADA', 'SUCCESS', $1)
                    RETURNING run_id
                    """,
                    len(fada_result.records),
                )
                await conn.executemany(
                    """
                    INSERT INTO raw_fada_monthly
                        (run_id, report_month, category, oem_name, volume_current)
                    VALUES ($1, $2, $3, $4, $5)
                    ON CONFLICT DO NOTHING
                    """,
                    [
                        (
                            run_id,
                            report_month,
                            rec.extra.get("segment", "UNKNOWN"),
                            rec.maker,
                            rec.registration_count,
                        )
                        for rec in fada_result.records
                    ],
                )

    log.info("reconcile.fada_loaded", oem_count=len(fada_volumes))

    # Step 3: Compute OEM-level reconciliation
    oem_recons: List[OEMReconciliation] = []
    flagged_count = 0

    for (oem_name, segment), vahan_vol in vahan_volumes.items():
        fada_vol = fada_volumes.get(oem_name, 0)
        if fada_vol < MIN_FADA_VOLUME:
            continue

        delta = vahan_vol - fada_vol
        delta_pct = abs(delta) / max(fada_vol, 1) * 100
        flagged = delta_pct > delta_threshold

        if flagged:
            flagged_count += 1

        oem_recons.append(OEMReconciliation(
            oem_name=oem_name,
            segment=segment,
            vahan_volume=vahan_vol,
            fada_volume=fada_vol,
            delta=delta,
            delta_pct=round(delta_pct, 2),
            flagged=flagged,
        ))

    # Compute overall delta
    total_vahan = sum(v for v in vahan_volumes.values())
    total_fada = sum(v for v in fada_volumes.values())
    total_delta_pct = (
        abs(total_vahan - total_fada) / max(total_fada, 1) * 100
        if total_fada > 0
        else 0.0
    )

    issues = []
    if flagged_count > 0:
        issues.append(
            f"{flagged_count} OEM(s) exceed delta threshold of {delta_threshold}%"
        )

    result = ReconciliationResult(
        report_month=report_month,
        passed=flagged_count == 0,
        total_delta_pct=round(total_delta_pct, 2),
        oem_reconciliations=oem_recons,
        issues=issues,
    )

    # Alert if reconciliation failed
    if not result.passed:
        alert_msg = (
            f"⚠️ VAHAN↔FADA Reconciliation {report_month.strftime('%Y-%m')}: "
            f"{flagged_count} OEM(s) flagged. "
            f"Total delta: {total_delta_pct:.1f}%"
        )
        await send_telegram_alert(settings=settings, message=alert_msg)

    log.info(
        "reconcile.complete",
        passed=result.passed,
        flagged=flagged_count,
        total_delta_pct=total_delta_pct,
    )
    return result
