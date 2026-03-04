"""
AutoQuant ETL — Base Connector (Adapter Pattern)
All data source connectors extend this ABC.
Swap VAHAN scraper for NAPIX API without changing warehouse, transforms, or frontend.
"""

from __future__ import annotations

import abc
from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, List, Optional, Any


class ConnectorSource(str, Enum):
    VAHAN = "VAHAN"
    FADA = "FADA"
    BSE = "BSE"
    SIAM = "SIAM"
    NAPIX = "NAPIX"


@dataclass
class ExtractParams:
    """Parameters for an extraction request."""
    period: str                       # 'YYYY-MM' for monthly, 'YYYY-MM-DD' for daily
    period_type: str = "month"        # 'month' | 'day'
    state: str = "All India"          # State filter; 'All India' for national
    y_axis_types: List[str] = field(
        default_factory=lambda: ["makerName", "vehicleClass"]
    )
    extra_params: Dict[str, Any] = field(default_factory=dict)


@dataclass
class RawRecord:
    """A single raw data record from a source connector."""
    maker: Optional[str] = None
    vehicle_class: Optional[str] = None
    fuel: Optional[str] = None
    registration_count: int = 0
    period: Optional[str] = None
    state: Optional[str] = None
    extra: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ExtractionResult:
    """Result of a connector extract() call."""
    source: ConnectorSource
    period: str
    records: List[RawRecord] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)
    warnings: List[str] = field(default_factory=list)


@dataclass
class ValidationResult:
    """Result of a connector validate() call."""
    passed: bool
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)


class BaseConnector(abc.ABC):
    """
    Abstract Base Class for all data source connectors.

    All connectors must implement:
      - extract(params)  → ExtractionResult
      - validate(data)   → ValidationResult
      - get_source_name()→ str
      - health_check()   → bool

    Connectors are used as async context managers:

        async with VahanConnector(settings) as vc:
            result = await vc.extract(params)

    This ensures browser/session resources are always cleaned up.
    """

    def __init__(self, settings) -> None:
        self.settings = settings
        self._initialized = False

    async def __aenter__(self) -> "BaseConnector":
        await self._setup()
        self._initialized = True
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        await self._teardown()
        self._initialized = False

    async def _setup(self) -> None:
        """Override to initialize resources (browser, HTTP session, etc.)"""
        pass

    async def _teardown(self) -> None:
        """Override to clean up resources."""
        pass

    @abc.abstractmethod
    async def extract(self, params: ExtractParams) -> ExtractionResult:
        """
        Extract raw data for the given period.

        Args:
            params: Extraction parameters

        Returns:
            ExtractionResult with raw records

        Raises:
            ConnectorError: if extraction fails unrecoverably
        """
        ...

    @abc.abstractmethod
    async def validate(self, data: ExtractionResult) -> ValidationResult:
        """
        Validate extracted data before passing to transforms.

        Args:
            data: ExtractionResult from extract()

        Returns:
            ValidationResult indicating pass/fail
        """
        ...

    @abc.abstractmethod
    def get_source_name(self) -> str:
        """Return the source name string (e.g. 'VAHAN')."""
        ...

    @abc.abstractmethod
    async def health_check(self) -> bool:
        """
        Perform a lightweight connectivity check.

        Returns:
            True if the source is reachable, False otherwise
        """
        ...


class ConnectorError(Exception):
    """Raised when a connector encounters an unrecoverable error."""

    def __init__(self, source: str, message: str, cause: Optional[Exception] = None):
        self.source = source
        self.cause = cause
        super().__init__(f"[{source}] {message}")
