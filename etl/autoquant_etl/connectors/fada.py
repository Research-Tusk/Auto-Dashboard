"""
AutoQuant ETL — FADA PDF Connector
====================================
Downloads and parses monthly FADA press release PDFs.
Extracts OEM-level registration data for PV, CV, and 2W segments.

FADA publishes monthly at: https://fada.in/press-release/
Typical filename pattern: FADA-Monthly-Sales-Data-[Month]-[Year].pdf

This connector:
  1. Downloads the PDF (from URL or local path)
  2. Extracts tables using pdfplumber
  3. Normalises column names and segment labels
  4. Returns RawRecord list for each OEM row

Note: FADA PDF layouts change occasionally. The parser uses flexible
column detection rather than hard-coded positions.
"""

from __future__ import annotations

import io
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import httpx
import pdfplumber
import structlog

from autoquant_etl.connectors.base import (
    BaseConnector,
    ConnectorSource,
    ExtractParams,
    ExtractionResult,
    RawRecord,
    ValidationResult,
)

logger = structlog.get_logger(__name__)

# Known FADA segment labels (normalised lowercase)
SEGMENT_PATTERNS = {
    "PV": ["passenger vehicle", "passenger car", "pv"],
    "CV": ["commercial vehicle", "cv", "commercial"],
    "2W": ["two wheeler", "two-wheeler", "2 wheeler", "2w"],
    "3W": ["three wheeler", "three-wheeler", "3 wheeler", "3w"],
}

# Column name aliases for flexible matching
VOLUME_COL_ALIASES = [
    "current month", "curr month", "sales", "volume", "units", "registrations"
]
PRIOR_YEAR_ALIASES = ["previous year", "prior year", "ly", "last year", "yago"]
SHARE_COL_ALIASES = ["market share", "share", "ms%", "mkt share"]


def _normalise_col(col: str) -> str:
    return re.sub(r"\s+", " ", col.lower().strip())


def _detect_segment(text: str) -> Optional[str]:
    """Detect segment from a table heading or row."""
    low = text.lower()
    for segment, patterns in SEGMENT_PATTERNS.items():
        if any(p in low for p in patterns):
            return segment
    return None


def _find_col_idx(headers: List[str], aliases: List[str]) -> Optional[int]:
    """Find column index by matching against alias list."""
    for i, h in enumerate(headers):
        nh = _normalise_col(h)
        if any(alias in nh for alias in aliases):
            return i
    return None


def _parse_int(value: str) -> Optional[int]:
    """Parse integer from string, handling commas and dashes."""
    if not value or value.strip() in ("-", "N/A", "", "na", "nil"):
        return None
    cleaned = re.sub(r"[,\s]", "", value.strip())
    try:
        return int(cleaned)
    except ValueError:
        return None


def _parse_float(value: str) -> Optional[float]:
    """Parse float from string, handling % signs."""
    if not value or value.strip() in ("-", "N/A", "", "na", "nil"):
        return None
    cleaned = value.replace("%", "").replace(",", "").strip()
    try:
        return float(cleaned)
    except ValueError:
        return None


@dataclass
class FadaRecord:
    """Intermediate parsed FADA record before normalisation."""
    segment: str
    oem_name: str
    volume_current: Optional[int]
    volume_prior_year: Optional[int]
    yoy_pct: Optional[float]
    market_share_pct: Optional[float]


class FadaConnector(BaseConnector):
    """
    FADA Monthly PDF Connector.

    Usage:
        async with FadaConnector(settings) as fc:
            params = ExtractParams(period='2025-12', ...)
            # Pass pdf_path or pdf_url via params.extra_params
            result = await fc.extract(params)
    """

    def get_source_name(self) -> str:
        return ConnectorSource.FADA.value

    async def health_check(self) -> bool:
        """Check if FADA website is reachable."""
        try:
            async with httpx.AsyncClient(timeout=10) as client:
                resp = await client.head("https://fada.in/")
                return resp.status_code < 500
        except Exception:
            return False

    async def extract(self, params: ExtractParams) -> ExtractionResult:
        """
        Extract FADA data from PDF.

        params.extra_params must contain one of:
          - 'pdf_path': local file path to PDF
          - 'pdf_url': URL to download PDF from
        """
        pdf_path = params.extra_params.get("pdf_path")
        pdf_url = params.extra_params.get("pdf_url")

        if not pdf_path and not pdf_url:
            raise ValueError(
                "FadaConnector.extract() requires 'pdf_path' or 'pdf_url' in extra_params"
            )

        # Download PDF if URL provided
        if pdf_url:
            pdf_bytes = await self._download_pdf(pdf_url)
            pdf_source = pdf_url
        else:
            pdf_bytes = Path(pdf_path).read_bytes()
            pdf_source = pdf_path

        logger.info("fada.extract_start", source=pdf_source, period=params.period)

        # Parse PDF
        fada_records = self._parse_pdf(pdf_bytes)
        logger.info("fada.parsed", records=len(fada_records))

        # Convert to RawRecord
        raw_records: List[RawRecord] = []
        for fr in fada_records:
            raw_records.append(RawRecord(
                maker=fr.oem_name,
                vehicle_class=fr.segment,
                fuel=None,  # FADA doesn't always provide fuel breakdown
                registration_count=fr.volume_current or 0,
                period=params.period,
                state="All India",
                extra={
                    "segment": fr.segment,
                    "volume_prior_year": fr.volume_prior_year,
                    "yoy_pct": fr.yoy_pct,
                    "market_share_pct": fr.market_share_pct,
                    "source_file": pdf_source,
                },
            ))

        return ExtractionResult(
            source=ConnectorSource.FADA,
            period=params.period,
            records=raw_records,
            metadata={"source_file": pdf_source, "total_records": len(raw_records)},
        )

    async def validate(self, data: ExtractionResult) -> ValidationResult:
        """Validate FADA extraction result."""
        errors = []
        warnings = []

        if not data.records:
            errors.append("No records extracted from FADA PDF")
            return ValidationResult(passed=False, errors=errors)

        # Check minimum record count
        if len(data.records) < 10:
            warnings.append(f"Fewer than 10 records extracted ({len(data.records)}). PDF may be malformed.")

        # Check for required segments
        segments_found = {r.extra.get("segment") for r in data.records}
        for required_segment in ["PV", "2W"]:
            if required_segment not in segments_found:
                warnings.append(f"Segment {required_segment} not found in FADA data")

        # Check for zero volumes
        zero_vol = sum(1 for r in data.records if r.registration_count == 0)
        if zero_vol > len(data.records) * 0.3:
            warnings.append(f"High zero-volume rate: {zero_vol}/{len(data.records)} records")

        return ValidationResult(
            passed=len(errors) == 0,
            errors=errors,
            warnings=warnings,
        )

    async def _download_pdf(self, url: str) -> bytes:
        """Download PDF from URL."""
        logger.info("fada.download_start", url=url)
        async with httpx.AsyncClient(timeout=60, follow_redirects=True) as client:
            resp = await client.get(url)
            resp.raise_for_status()
        logger.info("fada.download_done", size=len(resp.content))
        return resp.content

    def _parse_pdf(self, pdf_bytes: bytes) -> List[FadaRecord]:
        """Parse FADA PDF and extract OEM-level records."""
        records: List[FadaRecord] = []
        current_segment: Optional[str] = None

        with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
            for page_num, page in enumerate(pdf.pages, 1):
                tables = page.extract_tables()
                if not tables:
                    # Try to detect segment from page text
                    text = page.extract_text() or ""
                    detected = _detect_segment(text)
                    if detected:
                        current_segment = detected
                    continue

                for table in tables:
                    if not table:
                        continue

                    # Check if table heading reveals segment
                    text_above = page.extract_text() or ""
                    detected_seg = _detect_segment(text_above)
                    if detected_seg:
                        current_segment = detected_seg

                    # Find header row
                    header_row_idx = None
                    headers: List[str] = []
                    for i, row in enumerate(table):
                        if row and any(
                            cell and any(alias in _normalise_col(str(cell)) for alias in VOLUME_COL_ALIASES)
                            for cell in row
                        ):
                            header_row_idx = i
                            headers = [str(c or "") for c in row]
                            break

                    if header_row_idx is None:
                        # First row as fallback header
                        headers = [str(c or "") for c in (table[0] or [])]
                        header_row_idx = 0

                    # Detect column indices
                    name_col = 0  # OEM name is usually first column
                    vol_col = _find_col_idx(headers, VOLUME_COL_ALIASES)
                    prior_col = _find_col_idx(headers, PRIOR_YEAR_ALIASES)
                    share_col = _find_col_idx(headers, SHARE_COL_ALIASES)

                    if vol_col is None:
                        continue  # Can't parse without volume column

                    # Extract data rows
                    for row in table[header_row_idx + 1:]:
                        if not row:
                            continue

                        # Check if row is a segment header
                        row_text = " ".join(str(c or "") for c in row)
                        detected_seg = _detect_segment(row_text)
                        if detected_seg and len([c for c in row if c and str(c).strip()]) <= 2:
                            current_segment = detected_seg
                            continue

                        # Extract OEM name
                        if name_col >= len(row) or not row[name_col]:
                            continue
                        oem_name = str(row[name_col]).strip()
                        if not oem_name or oem_name.lower() in (
                            "total", "grand total", "industry", "segment total", "others"
                        ):
                            continue

                        # Extract volume fields
                        vol_current = _parse_int(str(row[vol_col]) if vol_col < len(row) else "")
                        if vol_current is None:
                            continue

                        vol_prior = _parse_int(
                            str(row[prior_col]) if prior_col is not None and prior_col < len(row) else ""
                        )
                        share_pct = _parse_float(
                            str(row[share_col]) if share_col is not None and share_col < len(row) else ""
                        )

                        # YoY calculation
                        yoy = None
                        if vol_prior and vol_prior > 0:
                            yoy = round((vol_current - vol_prior) / vol_prior * 100, 2)

                        records.append(FadaRecord(
                            segment=current_segment or "UNKNOWN",
                            oem_name=oem_name,
                            volume_current=vol_current,
                            volume_prior_year=vol_prior,
                            yoy_pct=yoy,
                            market_share_pct=share_pct,
                        ))

        logger.info("fada.parse_complete", records=len(records))
        return records
