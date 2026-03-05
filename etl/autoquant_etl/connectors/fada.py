"""
AutoQuant ETL — FADA PDF Connector
=====================================
Extracts monthly retail sales data from FADA (Federation of Automobile
Dealers Associations) PDF reports.

FADA publishes monthly retail sales data as PDF reports. This connector:
  1. Downloads the PDF (from URL or local path)
  2. Extracts tables using pdfplumber
  3. Parses segment × OEM × units structure
  4. Returns normalised RawRecord list

PDF table structure (typical FADA format):
  Segment | OEM Name | Units Retail | YoY%

Usage:
    async with FadaConnector(settings) as fc:
        params = ExtractParams(
            period='2026-02',
            extra_params={'pdf_url': 'https://fada.in/reports/feb26.pdf'}
        )
        result = await fc.extract(params)
"""

from __future__ import annotations

import io
import re
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import httpx
import structlog

from autoquant_etl.connectors.base import (
    BaseConnector,
    ConnectorSource,
    ExtractParams,
    ExtractionResult,
    RawRecord,
    ValidationResult,
)
from autoquant_etl.config import Settings

logger = structlog.get_logger(__name__)

# Segment name → canonical segment_code mapping
FADA_SEGMENT_MAP: Dict[str, str] = {
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
    "TRACTOR": "TRACTOR",
    "TRACTORS": "TRACTOR",
}


class FadaConnector(BaseConnector):
    """
    FADA PDF Report Connector.

    Extracts retail sales data from FADA monthly PDF reports.
    Supports both local file paths and remote URLs.

    The connector does not require browser setup — PDF extraction
    is done directly using pdfplumber.

    Usage:
        async with FadaConnector(settings) as fc:
            params = ExtractParams(
                period='2026-02',
                extra_params={
                    'pdf_url': 'https://...',   # OR
                    'pdf_path': '/path/to/report.pdf'
                }
            )
            result = await fc.extract(params)
    """

    def __init__(self, settings: Settings) -> None:
        super().__init__(settings)
        self._http_client: Optional[httpx.AsyncClient] = None

    async def _setup(self) -> None:
        """Initialise the HTTP client for PDF downloads."""
        self._http_client = httpx.AsyncClient(
            timeout=60.0,
            follow_redirects=True,
            headers={"User-Agent": "AutoQuant-ETL/1.0"},
        )

    async def _teardown(self) -> None:
        """Close the HTTP client."""
        if self._http_client:
            await self._http_client.aclose()

    def get_source_name(self) -> str:
        return ConnectorSource.FADA.value

    async def health_check(self) -> bool:
        """
        Check FADA website reachability.

        Returns True if the FADA website (fada.in) responds with HTTP < 500.
        """
        try:
            assert self._http_client is not None
            resp = await self._http_client.get(
                "https://fada.in", timeout=15.0
            )
            return resp.status_code < 500
        except Exception as exc:
            logger.warning("fada.health_check_failed", error=str(exc))
            return False

    async def extract(self, params: ExtractParams) -> ExtractionResult:
        """
        Extract retail sales data from a FADA PDF report.

        Reads PDF source from params.extra_params:
          - 'pdf_url': URL to download
          - 'pdf_path': local file path

        Args:
            params: ExtractParams with period and extra_params

        Returns:
            ExtractionResult with RawRecord list
        """
        pdf_url: Optional[str] = params.extra_params.get("pdf_url")
        pdf_path: Optional[str] = params.extra_params.get("pdf_path")

        log = logger.bind(period=params.period, pdf_url=pdf_url, pdf_path=pdf_path)
        log.info("fada.extract_start")

        records: List[RawRecord] = []
        warnings: List[str] = []

        try:
            pdf_bytes = await self._fetch_pdf(pdf_url=pdf_url, pdf_path=pdf_path)
            records, table_warnings = self._parse_pdf(
                pdf_bytes=pdf_bytes,
                period=params.period,
            )
            warnings.extend(table_warnings)
        except Exception as exc:
            log.error("fada.extract_failed", error=str(exc))
            return ExtractionResult(
                source=ConnectorSource.FADA,
                period=params.period,
                records=[],
                warnings=[f"Extraction failed: {exc}"],
            )

        log.info("fada.extract_done", records=len(records))

        return ExtractionResult(
            source=ConnectorSource.FADA,
            period=params.period,
            records=records,
            metadata={
                "pdf_url": pdf_url,
                "pdf_path": pdf_path,
                "record_count": len(records),
            },
            warnings=warnings,
        )

    async def validate(self, data: ExtractionResult) -> ValidationResult:
        """
        Validate FADA extraction result.

        Checks:
        - Records exist
        - At least one recognised segment present
        - No negative counts
        """
        errors: List[str] = []
        warnings: List[str] = []

        if not data.records:
            errors.append("No records extracted from FADA PDF")
            return ValidationResult(passed=False, errors=errors)

        segments = {r.extra.get("segment_code") for r in data.records if r.extra}
        known = segments & set(FADA_SEGMENT_MAP.values())
        if not known:
            warnings.append(
                f"No recognised segments found. Got: {segments}"
            )

        negatives = [r for r in data.records if r.registration_count < 0]
        if negatives:
            errors.append(f"{len(negatives)} records have negative counts")

        return ValidationResult(
            passed=len(errors) == 0,
            errors=errors,
            warnings=warnings,
        )

    async def _fetch_pdf(
        self,
        pdf_url: Optional[str],
        pdf_path: Optional[str],
    ) -> bytes:
        """Download or read the PDF and return raw bytes."""
        if pdf_url:
            assert self._http_client is not None
            resp = await self._http_client.get(pdf_url)
            resp.raise_for_status()
            logger.debug("fada.pdf_downloaded", url=pdf_url, size=len(resp.content))
            return resp.content

        if pdf_path:
            data = Path(pdf_path).read_bytes()
            logger.debug("fada.pdf_read_local", path=pdf_path, size=len(data))
            return data

        raise ValueError("Either pdf_url or pdf_path must be provided in extra_params")

    def _parse_pdf(
        self,
        pdf_bytes: bytes,
        period: str,
    ) -> Tuple[List[RawRecord], List[str]]:
        """
        Parse all tables in the PDF and extract RawRecord list.

        Args:
            pdf_bytes: raw PDF content
            period: 'YYYY-MM' period string

        Returns:
            (records, warnings)
        """
        try:
            import pdfplumber
        except ImportError:
            raise RuntimeError(
                "pdfplumber is required for FADA PDF extraction. "
                "Install with: pip install pdfplumber"
            )

        records: List[RawRecord] = []
        warnings: List[str] = []

        with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
            for page_num, page in enumerate(pdf.pages, start=1):
                tables = page.extract_tables()
                for table_idx, table in enumerate(tables):
                    if not table or len(table) < 2:
                        continue

                    page_records, page_warnings = self._parse_table(
                        table=table,
                        period=period,
                        page_num=page_num,
                        table_idx=table_idx,
                    )
                    records.extend(page_records)
                    warnings.extend(page_warnings)

        return records, warnings

    def _parse_table(
        self,
        table: List[List[Optional[str]]],
        period: str,
        page_num: int,
        table_idx: int,
    ) -> Tuple[List[RawRecord], List[str]]:
        """
        Parse a single pdfplumber table into RawRecord list.

        Attempts to identify:
          - Column 0: segment header or row identifier
          - Column 1: OEM name
          - Subsequent columns: unit counts, YoY%

        Returns (records, warnings).
        """
        records: List[RawRecord] = []
        warnings: List[str] = []
        current_segment: Optional[str] = None

        # Try to identify header row
        header = [str(c).strip().upper() if c else "" for c in (table[0] or [])]

        # Find the units column index (first numeric-looking header or "UNITS" / "TOTAL")
        units_col_idx = self._find_units_column(header)

        for row_idx, row in enumerate(table[1:], start=1):
            if not row:
                continue

            cells = [str(c).strip() if c else "" for c in row]
            if not any(cells):
                continue

            first = cells[0].upper() if cells[0] else ""

            # Check for segment header row
            seg_code = FADA_SEGMENT_MAP.get(first)
            if seg_code:
                current_segment = seg_code
                continue

            # Skip total / subtotal rows
            if any(
                kw in first
                for kw in ("TOTAL", "GRAND TOTAL", "SUB TOTAL", "SUBTOTAL", "INDUSTRY")
            ):
                continue

            if current_segment is None:
                continue

            # Parse OEM name (usually column 1)
            oem_name = cells[1] if len(cells) > 1 else cells[0]
            if not oem_name:
                continue

            # Parse unit count
            units = 0
            if units_col_idx is not None and units_col_idx < len(cells):
                units = self._parse_int(cells[units_col_idx])
            else:
                # Scan for first integer-like cell after OEM name
                for cell in cells[2:]:
                    val = self._parse_int(cell)
                    if val > 0:
                        units = val
                        break

            if units == 0:
                continue

            records.append(
                RawRecord(
                    maker=oem_name,
                    vehicle_class=None,
                    fuel=None,
                    registration_count=units,
                    period=period,
                    state="All India",
                    extra={
                        "segment_code": current_segment,
                        "source": "FADA",
                        "page": page_num,
                    },
                )
            )

        return records, warnings

    @staticmethod
    def _find_units_column(header: List[str]) -> Optional[int]:
        """Find the column index most likely containing unit counts."""
        for kw in ("UNITS", "TOTAL", "RETAIL", "SALES", "VOLUME"):
            for idx, col in enumerate(header):
                if kw in col:
                    return idx
        return None

    @staticmethod
    def _parse_int(text: str) -> int:
        """Parse an integer from a cell value, removing commas and spaces."""
        cleaned = re.sub(r"[,\s\xa0]", "", text.strip())
        try:
            return max(0, int(float(cleaned)))
        except (ValueError, TypeError):
            return 0
