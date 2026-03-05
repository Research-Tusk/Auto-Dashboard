"""
AutoQuant ETL — Connectors Package
=====================================
Re-exports connector classes and base types for convenient importing.
"""

from autoquant_etl.connectors.base import (
    BaseConnector,
    ConnectorSource,
    ConnectorError,
    ExtractParams,
    ExtractionResult,
    RawRecord,
    ValidationResult,
)
from autoquant_etl.connectors.vahan import VahanConnector
from autoquant_etl.connectors.fada import FadaConnector

__all__ = [
    "BaseConnector",
    "ConnectorSource",
    "ConnectorError",
    "ExtractParams",
    "ExtractionResult",
    "RawRecord",
    "ValidationResult",
    "VahanConnector",
    "FadaConnector",
]
