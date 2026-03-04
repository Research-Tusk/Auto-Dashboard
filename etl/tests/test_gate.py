"""
Tests for the validation gate.
"""

from __future__ import annotations

import pytest
from unittest.mock import AsyncMock, patch

from autoquant_etl.connectors.base import RawRecord
from autoquant_etl.validators.gate import (
    run_validation_gate,
    GateConfig,
    GateResult,
    _check_min_row_count,
    _check_maker_coverage,
    _check_fuel_coverage,
    _check_total_volume_sanity,
    _check_duplicate_rows,
    _check_negative_delta,
)


def make_records(n: int = 100, volume: int = 1000) -> list[RawRecord]:
    """Generate n records with given volume."""
    return [
        RawRecord(
            maker=f"MAKER_{i}",
            vehicle_class="Motor Car",
            fuel="PETROL",
            registration_count=volume,
            period="2025-12",
            state="All India",
        )
        for i in range(n)
    ]


class TestMinRowCount:
    def test_pass(self):
        records = make_records(100)
        result = _check_min_row_count(records, GateConfig())
        assert result.passed

    def test_fail(self):
        records = make_records(10)
        result = _check_min_row_count(records, GateConfig())
        assert not result.passed

    def test_custom_threshold(self):
        records = make_records(30)
        result = _check_min_row_count(records, GateConfig(min_row_count=20))
        assert result.passed


class TestMakerCoverage:
    def test_full_coverage(self):
        records = make_records(100)
        result = _check_maker_coverage(records, GateConfig())
        assert result.passed

    def test_partial_coverage_pass(self):
        records = make_records(80)
        records += [
            RawRecord(maker=None, registration_count=10, period="2025-12")
            for _ in range(20)
        ]
        # 80% volume from mapped makers
        result = _check_maker_coverage(records, GateConfig(min_maker_coverage=0.8))
        assert result.passed

    def test_zero_volume(self):
        records = [RawRecord(maker="X", registration_count=0, period="2025-12")]
        result = _check_maker_coverage(records, GateConfig())
        assert not result.passed


class TestFuelCoverage:
    def test_pass_all_known(self):
        records = make_records(50)
        result = _check_fuel_coverage(records, GateConfig())
        assert result.passed

    def test_allow_zero_fuel(self):
        records = [RawRecord(maker="X", fuel=None, registration_count=100, period="2025-12")]
        result = _check_fuel_coverage(records, GateConfig(allow_zero_fuel=True))
        assert result.passed

    def test_unknown_fuel(self):
        records = [
            RawRecord(maker="X", fuel="UNKNOWN_FUEL", registration_count=100, period="2025-12")
            for _ in range(100)
        ]
        result = _check_fuel_coverage(records, GateConfig())
        assert not result.passed


class TestDuplicateRows:
    def test_no_duplicates(self):
        records = make_records(50)
        result = _check_duplicate_rows(records)
        assert result.passed

    def test_with_duplicates(self):
        record = RawRecord(
            maker="MARUTI", vehicle_class="Motor Car", fuel="PETROL",
            registration_count=100, period="2025-12", state="All India"
        )
        result = _check_duplicate_rows([record, record])
        assert not result.passed


class TestNegativeDelta:
    def test_all_positive(self):
        records = make_records(10)
        result = _check_negative_delta(records)
        assert result.passed

    def test_has_negative(self):
        records = [RawRecord(maker="X", registration_count=-5, period="2025-12")]
        result = _check_negative_delta(records)
        assert not result.passed


class TestRunValidationGate:
    @pytest.mark.asyncio
    async def test_all_checks_pass(self, mock_pool, sample_raw_records):
        """Gate should pass with valid records."""
        with patch(
            "autoquant_etl.validators.gate._check_zscore_anomaly",
            new_callable=AsyncMock,
            return_value=type("GC", (), {"name": "zscore", "passed": True, "detail": "ok"})()
        ):
            result = await run_validation_gate(
                records=sample_raw_records * 20,  # 100+ records
                pool=mock_pool,
                month="2025-12",
                config=GateConfig(min_total_volume=100),  # Low threshold for test
            )
        assert result.passed
