"""AutoQuant ETL — Connectors package."""

from autoquant_etl.connectors.base import (
    BaseConnector,
    ConnectorSource,
    ExtractParams,
    RawRecord,
    ExtractionResult,
    ValidationResult,
)

__all__ = [
    "BaseConnector",
    "ConnectorSource",
    "ExtractParams",
    "RawRecord",
    "ExtractionResult",
    "ValidationResult",
]
