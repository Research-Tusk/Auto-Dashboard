"""
AutoQuant ETL — VAHAN vs FADA Reconciliation
=============================================
Compares VAHAN registration data against FADA (Federation of Automobile
Dealers Associations) monthly retail sales data to detect discrepancies.

VAHAN = government registration database (actual chassis numbers registered)
FADA = dealer retail sales (invoice from dealer to customer)

These should be closely aligned but differ due to:
  - Timing (transit inventory, month-end dealer stock)
  - Scope differences (certain vehicle types)

A discrepancy > 5% per OEM × segment triggers a flag.

Data sources:
  1. Local PDF path (--pdf-path): extract using pdfplumber
  2. Remote PDF URL (--pdf-url): download then extract
  3. Database (default): query raw_fada_monthly table
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from typing import Dict, List, Optional, Tuple
from pathlib import Path

import asyncpg
import structlog

logger = structlog.get_logger(__name__)

# Discrepancy threshold (percentage)
DISCREPANCY_THRESHOLD_PCT = 5.0


@dataclass
class ReconciliationResult:
    """Result of a VAHAN vs FADA reconciliation run."""
    passed: bool
    total_delta_pct: float
    issues: List[str] = field(default_factory=list)
    vahan_total: int = 0
    fada_total: int = 0
    segments_compared: int = 0
    dry_run: bool = False


async def run_reconciliation(
    pool: asyncpg.Pool,
    settings,
    report_month: date,
    pdf_path: Optional[str] = None,
    pdf_url: Optional[str] = None,
    dry_run: bool = False,
    verbose: bool = False,
) -> ReconciliationResult:
    """
    Run VAHAN vs FADA reconciliation for a given report month.

    Args:
        pool: asyncpg connection pool
        settings: application Settings
        report_month: first day of the month to reconcile (date object)
        pdf_path: optional local path to FADA PDF report
        pdf_url: optional URL to download FADA PDF from
        dry_run: if True, compute differences but skip writing to DB
        verbose: enable verbose logging

    Returns:
        ReconciliationResult with pass/fail status and issue list
    """
    period = report_month.strftime("%Y-%m")
    log = logger.bind(period=period, dry_run=dry_run)
    log.info("reconcile.start")

    issues: List[str] = []

    # -------------------------------------------------------------------------
    # Step 1: Get FADA data (PDF or DB)
    # -------------------------------------------------------------------------
    fada_data: Dict[str, int] = {}

    if pdf_path or pdf_url:
        try:
            fada_data = await _extract_fada_from_pdf(
                pdf_path=pdf_path,
                pdf_url=pdf_url,
                verbose=verbose,
            )
            log.info("reconcile.fada_from_pdf", records=len(fada_data))
        except Exception as exc:
            log.warning("reconcile.pdf_extraction_failed", error=str(exc))
            issues.append(f"FADA PDF extraction failed: {exc}")
            # Fall back to DB
            fada_data = await _get_fada_from_db(pool=pool, report_month=report_month)
    else:
        fada_data = await _get_fada_from_db(pool=pool, report_month=report_month)

    if not fada_data:
        log.warning("reconcile.no_fada_data")
        return ReconciliationResult(
            passed=False,
            total_delta_pct=100.0,
            issues=["No FADA data available for comparison"],
            dry_run=dry_run,
        )

    # -------------------------------------------------------------------------
    # Step 2: Get VAHAN data from fact_monthly_registrations
    # -------------------------------------------------------------------------
    vahan_data: Dict[str, int] = await _get_vahan_data(
        pool=pool, report_month=report_month
    )

    if not vahan_data:
        log.warning("reconcile.no_vahan_data")
        return ReconciliationResult(
            passed=False,
            total_delta_pct=100.0,
            issues=["No VAHAN data available for comparison"],
            dry_run=dry_run,
        )

    # -------------------------------------------------------------------------
    # Step 3: Compare segment totals
    # -------------------------------------------------------------------------
    fada_total = sum(fada_data.values())
    vahan_total = sum(vahan_data.values())

    # Segment-level comparison
    all_segments = set(fada_data.keys()) | set(vahan_data.keys())
    segments_compared = 0

    for segment in sorted(all_segments):
        fada_units = fada_data.get(segment, 0)
        vahan_units = vahan_data.get(segment, 0)

        if fada_units == 0 and vahan_units == 0:
            continue

        segments_compared += 1

        if fada_units == 0:
            delta_pct = 100.0
        else:
            delta_pct = abs(vahan_units - fada_units) / fada_units * 100

        if verbose:
            log.debug(
                "reconcile.segment_delta",
                segment=segment,
                vahan=vahan_units,
                fada=fada_units,
                delta_pct=round(delta_pct, 2),
            )

        if delta_pct > DISCREPANCY_THRESHOLD_PCT:
            issues.append(
                f"{segment}: VAHAN={vahan_units:,} vs FADA={fada_units:,} "
                f"({delta_pct:.1f}% delta — exceeds {DISCREPANCY_THRESHOLD_PCT}% threshold)"
            )

    # Overall delta
    if fada_total > 0:
        total_delta_pct = abs(vahan_total - fada_total) / fada_total * 100
    elif vahan_total > 0:
        total_delta_pct = 100.0
    else:
        total_delta_pct = 0.0

    passed = len(issues) == 0

    log.info(
        "reconcile.complete",
        passed=passed,
        vahan_total=vahan_total,
        fada_total=fada_total,
        total_delta_pct=round(total_delta_pct, 2),
        issues=len(issues),
    )

    # Persist FADA data to raw_fada_monthly if it came from PDF and not dry_run
    if (pdf_path or pdf_url) and not dry_run and fada_data:
        await _persist_fada_data(
            pool=pool,
            report_month=report_month,
            fada_data=fada_data,
            pdf_url=pdf_url or pdf_path or "",
        )

    return ReconciliationResult(
        passed=passed,
        total_delta_pct=round(total_delta_pct, 2),
        issues=issues,
        vahan_total=vahan_total,
        fada_total=fada_total,
        segments_compared=segments_compared,
        dry_run=dry_run,
    )


async def _get_fada_from_db(
    pool: asyncpg.Pool,
    report_month: date,
) -> Dict[str, int]:
    """Query raw_fada_monthly for the given month, aggregated by segment."""
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT segment, SUM(units_retail) AS total
            FROM raw_fada_monthly
            WHERE report_month = $1
            GROUP BY segment
            """,
            report_month,
        )
    return {row["segment"]: int(row["total"]) for row in rows if row["total"]}


async def _get_vahan_data(
    pool: asyncpg.Pool,
    report_month: date,
) -> Dict[str, int]:
    """Query fact_monthly_registrations joined to dim_segment for the given month."""
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT s.segment_code, SUM(mr.units) AS total
            FROM fact_monthly_registrations mr
            JOIN dim_segment s ON s.segment_id = mr.segment_id
            WHERE mr.month_key = $1
            GROUP BY s.segment_code
            """,
            report_month,
        )
    return {row["segment_code"]: int(row["total"]) for row in rows if row["total"]}


async def _extract_fada_from_pdf(
    pdf_path: Optional[str],
    pdf_url: Optional[str],
    verbose: bool = False,
) -> Dict[str, int]:
    """
    Extract FADA segment data from a PDF file.

    Downloads the PDF if a URL is provided. Parses the first table
    found in the document, attempting to identify segment and unit columns.

    Returns:
        Dict mapping segment_code → total units
    """
    import io
    import pdfplumber
    import httpx

    pdf_bytes: bytes

    if pdf_url:
        async with httpx.AsyncClient(timeout=60.0) as client:
            resp = await client.get(pdf_url)
            resp.raise_for_status()
            pdf_bytes = resp.content
        logger.debug("reconcile.pdf_downloaded", url=pdf_url, size=len(pdf_bytes))
    elif pdf_path:
        pdf_bytes = Path(pdf_path).read_bytes()
        logger.debug("reconcile.pdf_read_local", path=pdf_path, size=len(pdf_bytes))
    else:
        raise ValueError("Either pdf_path or pdf_url must be provided")

    segment_totals: Dict[str, int] = {}

    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        for page in pdf.pages:
            tables = page.extract_tables()
            for table in tables:
                if not table or len(table) < 2:
                    continue

                # Attempt to parse the table
                parsed = _parse_fada_table(table, verbose=verbose)
                for segment, units in parsed.items():
                    segment_totals[segment] = segment_totals.get(segment, 0) + units

            if segment_totals:
                break  # Found data on first page with tables

    return segment_totals


def _parse_fada_table(
    table: List[List[Optional[str]]],
    verbose: bool = False,
) -> Dict[str, int]:
    """
    Parse a pdfplumber table into segment → units mapping.

    FADA PDF tables typically have columns:
      Segment | OEM | Units | YoY%

    This parser looks for rows where the first column matches known
    segment codes or the second column looks like a unit count.
    """
    # Known segment labels in FADA PDFs
    SEGMENT_MAP = {
        "PASSENGER VEHICLE": "PV",
        "PASSENGER VEHICLES": "PV",
        "PV": "PV",
        "COMMERCIAL VEHICLE": "CV",
        "COMMERCIAL VEHICLES": "CV",
        "CV": "CV",
        "TWO WHEELER": "2W",
        "TWO WHEELERS": "2W",
        "2-WHEELER": "2W",
        "2W": "2W",
        "THREE WHEELER": "3W",
        "THREE WHEELERS": "3W",
        "3W": "3W",
    }

    result: Dict[str, int] = {}
    current_segment: Optional[str] = None

    for row in table:
        if not row:
            continue

        cells = [str(c).strip() if c else "" for c in row]
        first = cells[0].upper() if cells else ""

        # Check if this row is a segment header
        seg = SEGMENT_MAP.get(first)
        if seg:
            current_segment = seg
            continue

        if current_segment is None:
            continue

        # Try to find a "Total" or aggregate row
        row_text = " ".join(cells).upper()
        if "TOTAL" in row_text or "GRAND TOTAL" in row_text:
            # Find the numeric cell
            for cell in cells[1:]:
                cleaned = cell.replace(",", "").replace(" ", "")
                if cleaned.isdigit():
                    result[current_segment] = result.get(current_segment, 0) + int(cleaned)
                    break

    return result


async def _persist_fada_data(
    pool: asyncpg.Pool,
    report_month: date,
    fada_data: Dict[str, int],
    pdf_url: str,
) -> None:
    """Persist extracted FADA data to raw_fada_monthly."""
    async with pool.acquire() as conn:
        for segment, units in fada_data.items():
            await conn.execute(
                """
                INSERT INTO raw_fada_monthly
                    (report_month, segment, oem_name, units_retail, source_pdf_url)
                VALUES ($1, $2, 'ALL', $3, $4)
                ON CONFLICT DO NOTHING
                """,
                report_month,
                segment,
                units,
                pdf_url,
            )
    logger.info("reconcile.fada_persisted", count=len(fada_data))
