"""
AutoQuant ETL — VAHAN Dashboard Connector (Playwright-based)
============================================================
Extracts aggregated vehicle registration counts from the VAHAN4 dashboard
(https://vahan.parivahan.gov.in/vahan4dashboard/).

The VAHAN dashboard is a JSF (JavaServer Faces) application.
It does NOT expose a REST API — all interactions are through form submissions
and partial page updates (AJAX).

Extraction strategy:
  1. Navigate to the dashboard
  2. Set filters: state, date/month, Y-axis type (makerName / vehicleClass)
  3. Wait for table to render
  4. Read each maker row from the DOM
  5. Repeat for each Y-axis type needed

This connector is designed to be swapped for the NAPIX official API feed
when it becomes available, without requiring changes to transforms or frontend.
"""

from __future__ import annotations

import asyncio
import re
from typing import Any, Dict, List, Optional

from playwright.async_api import async_playwright, Browser, Page, Playwright
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

# VAHAN dashboard URL
VAHAN_DASHBOARD_URL = "https://vahan.parivahan.gov.in/vahan4dashboard/vahan/view/reportview.xhtml"

# Selectors (may need updating if VAHAN JSF component IDs change)
STATE_DROPDOWN_ID = "selectedStateUID"
Y_AXIS_DROPDOWN_ID = "yaxisVar"
DATE_INPUT_ID = "selectedDate"
GO_BUTTON_ID = "j_idt47"  # "Go" / Submit button
TABLE_SELECTOR = "table.dataTable, #groupingTable, .ui-datatable"
ROW_SELECTOR = "tbody tr"

# Y-axis type mapping
Y_AXIS_TYPES = {
    "makerName": "Maker",
    "vehicleClass": "Vehicle Class",
    "fuelType": "Fuel Type",
    "normType": "Norm Type",
}

# State dropdown values
STATE_VALUES = {
    "All India": "0",
    "Andhra Pradesh": "AP",
    # ... extend as needed for state drill-down
}

MAX_RETRIES = 3
PAGE_LOAD_TIMEOUT_MS = 60_000
TABLE_WAIT_TIMEOUT_MS = 45_000


class VahanConnector(BaseConnector):
    """
    VAHAN Dashboard Connector using Playwright.

    Extracts aggregated registration counts from the VAHAN4 dashboard.
    The dashboard is a JSF application — interaction is via DOM manipulation.

    Usage:
        async with VahanConnector(settings) as vc:
            params = ExtractParams(period='2026-03', state='All India')
            result = await vc.extract(params)
    """

    def __init__(self, settings: Settings) -> None:
        super().__init__(settings)
        self._playwright: Optional[Playwright] = None
        self._browser: Optional[Browser] = None

    async def _setup(self) -> None:
        """Launch Playwright browser."""
        self._playwright = await async_playwright().start()
        self._browser = await self._playwright.chromium.launch(
            headless=self.settings.playwright_headless,
            args=["--no-sandbox", "--disable-dev-shm-usage"],
        )
        logger.info("vahan.browser_launched")

    async def _teardown(self) -> None:
        """Close browser and Playwright."""
        if self._browser:
            await self._browser.close()
        if self._playwright:
            await self._playwright.stop()
        logger.info("vahan.browser_closed")

    def get_source_name(self) -> str:
        return ConnectorSource.VAHAN.value

    async def health_check(self) -> bool:
        """Check if VAHAN dashboard is reachable."""
        try:
            page = await self._browser.new_page()
            response = await page.goto(
                VAHAN_DASHBOARD_URL,
                timeout=PAGE_LOAD_TIMEOUT_MS,
                wait_until="domcontentloaded",
            )
            await page.close()
            return response is not None and response.status < 500
        except Exception as exc:
            logger.warning("vahan.health_check_failed", error=str(exc))
            return False

    async def extract(self, params: ExtractParams) -> ExtractionResult:
        """
        Extract registration data from VAHAN dashboard.

        Runs multiple queries (one per y_axis_type) and merges results.

        Args:
            params: ExtractParams with period, state, y_axis_types

        Returns:
            ExtractionResult with merged RawRecord list
        """
        all_records: List[RawRecord] = []
        metadata: Dict[str, Any] = {}
        warnings: List[str] = []

        for y_axis_type in params.y_axis_types:
            log = logger.bind(
                period=params.period,
                state=params.state,
                y_axis=y_axis_type,
            )
            log.info("vahan.extract_start")

            for attempt in range(1, MAX_RETRIES + 1):
                try:
                    records, meta = await self._extract_single(
                        period=params.period,
                        state=params.state,
                        y_axis_type=y_axis_type,
                    )
                    all_records.extend(records)
                    metadata[y_axis_type] = meta
                    log.info("vahan.extract_done", records=len(records), attempt=attempt)
                    break
                except Exception as exc:
                    log.warning(
                        "vahan.extract_retry",
                        attempt=attempt,
                        max_retries=MAX_RETRIES,
                        error=str(exc),
                    )
                    if attempt == MAX_RETRIES:
                        log.error("vahan.extract_failed", error=str(exc))
                        warnings.append(f"{y_axis_type} extraction failed: {exc}")
                    else:
                        await asyncio.sleep(5 * attempt)

        return ExtractionResult(
            source=ConnectorSource.VAHAN,
            period=params.period,
            records=all_records,
            metadata=metadata,
            warnings=warnings,
        )

    async def validate(self, data: ExtractionResult) -> ValidationResult:
        """Validate VAHAN extraction result."""
        errors = []
        warnings = []

        if not data.records:
            errors.append("No records extracted from VAHAN")
            return ValidationResult(passed=False, errors=errors)

        # Check minimum maker count
        makers = {r.maker for r in data.records if r.maker}
        if len(makers) < 5:
            warnings.append(f"Only {len(makers)} unique makers found (expected 20+)")

        # Check for negative counts
        neg = [r for r in data.records if r.registration_count < 0]
        if neg:
            errors.append(f"{len(neg)} records have negative registration counts")

        return ValidationResult(
            passed=len(errors) == 0,
            errors=errors,
            warnings=warnings,
        )

    async def _extract_single(
        self,
        period: str,
        state: str,
        y_axis_type: str,
    ) -> tuple[List[RawRecord], Dict[str, Any]]:
        """
        Extract data for a single y_axis_type query.

        Returns:
            (records, metadata)
        """
        page = await self._browser.new_page()
        try:
            # Navigate to dashboard
            await page.goto(
                VAHAN_DASHBOARD_URL,
                timeout=PAGE_LOAD_TIMEOUT_MS,
                wait_until="networkidle",
            )

            # Wait for dropdowns to be available
            await page.wait_for_selector(f"#{Y_AXIS_DROPDOWN_ID}", timeout=TABLE_WAIT_TIMEOUT_MS)

            # Set Y-axis type
            await page.select_option(f"#{Y_AXIS_DROPDOWN_ID}", value=y_axis_type)
            await page.wait_for_load_state("networkidle", timeout=TABLE_WAIT_TIMEOUT_MS)

            # Set state
            state_value = STATE_VALUES.get(state, "0")
            await page.select_option(f"#{STATE_DROPDOWN_ID}", value=state_value)
            await page.wait_for_load_state("networkidle", timeout=TABLE_WAIT_TIMEOUT_MS)

            # Set date/month
            # VAHAN uses a date picker; format varies by view
            # For monthly view: set to first day of the month
            year, mon = map(int, period.split("-"))
            date_str = f"{mon:02d}/{year}"
            await page.fill(f"#{DATE_INPUT_ID}", date_str)

            # Click Go
            await page.click(f"#{GO_BUTTON_ID}")
            await page.wait_for_selector(TABLE_SELECTOR, timeout=TABLE_WAIT_TIMEOUT_MS)
            await page.wait_for_load_state("networkidle", timeout=TABLE_WAIT_TIMEOUT_MS)

            # Extract table rows
            records = await self._parse_table(page, period=period, state=state, y_axis_type=y_axis_type)

            meta = {
                "url": page.url,
                "record_count": len(records),
                "period": period,
                "state": state,
                "y_axis": y_axis_type,
            }
            return records, meta

        finally:
            await page.close()

    async def _parse_table(
        self,
        page: Page,
        period: str,
        state: str,
        y_axis_type: str,
    ) -> List[RawRecord]:
        """Parse the registration table from the current page."""
        records: List[RawRecord] = []

        rows = await page.query_selector_all(ROW_SELECTOR)
        for row in rows:
            cells = await row.query_selector_all("td")
            if len(cells) < 2:
                continue

            texts = [await cell.inner_text() for cell in cells]
            texts = [t.strip() for t in texts]

            # Skip header or total rows
            if not texts[0] or texts[0].lower() in (
                "s.no", "sr no", "sl.no", "total", "grand total", "#"
            ):
                continue

            # VAHAN table format: [SrNo, MakerName, Count] or [SrNo, Class, Fuel, Count]
            if y_axis_type == "makerName":
                maker = texts[1] if len(texts) > 1 else None
                count_str = texts[-1]  # Count is usually last column
                records.append(RawRecord(
                    maker=maker,
                    vehicle_class=None,
                    fuel=None,
                    registration_count=self._parse_count(count_str),
                    period=period,
                    state=state,
                ))
            elif y_axis_type == "vehicleClass":
                vehicle_class = texts[1] if len(texts) > 1 else None
                count_str = texts[-1]
                records.append(RawRecord(
                    maker=None,
                    vehicle_class=vehicle_class,
                    fuel=None,
                    registration_count=self._parse_count(count_str),
                    period=period,
                    state=state,
                ))
            elif y_axis_type == "fuelType":
                fuel = texts[1] if len(texts) > 1 else None
                count_str = texts[-1]
                records.append(RawRecord(
                    maker=None,
                    vehicle_class=None,
                    fuel=fuel,
                    registration_count=self._parse_count(count_str),
                    period=period,
                    state=state,
                ))
            else:
                # Generic fallback
                records.append(RawRecord(
                    maker=texts[1] if len(texts) > 1 else None,
                    registration_count=self._parse_count(texts[-1]),
                    period=period,
                    state=state,
                    extra={"y_axis": y_axis_type, "raw": texts},
                ))

        return records

    @staticmethod
    def _parse_count(text: str) -> int:
        """Parse registration count from text, removing commas."""
        cleaned = re.sub(r"[,\s]", "", text.strip())
        try:
            return max(0, int(cleaned))
        except ValueError:
            return 0
