"""
Tests for the backfill orchestrator.
"""

from __future__ import annotations

import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from autoquant_etl.backfill import _month_range, BackfillResult, run_backfill


class TestMonthRange:
    """Tests for _month_range helper."""

    def test_single_month(self):
        result = _month_range("2025-12", "2025-12")
        assert result == ["2025-12"]

    def test_quarter_range(self):
        result = _month_range("2025-10", "2025-12")
        assert result == ["2025-10", "2025-11", "2025-12"]

    def test_cross_year(self):
        result = _month_range("2025-11", "2026-02")
        assert result == ["2025-11", "2025-12", "2026-01", "2026-02"]

    def test_fy_year(self):
        result = _month_range("2025-04", "2026-03")
        assert len(result) == 12
        assert result[0] == "2025-04"
        assert result[-1] == "2026-03"

    def test_invalid_range(self):
        # from > to returns empty list
        result = _month_range("2025-12", "2025-10")
        assert result == []


class TestBackfillDryRun:
    """Tests for run_backfill with dry_run=True."""

    @pytest.mark.asyncio
    async def test_dry_run_skips_db_writes(self, mock_pool, settings):
        """Dry run should not write to DB."""
        with patch("autoquant_etl.backfill.VahanConnector") as MockVC, \
             patch("autoquant_etl.backfill.run_validation_gate") as mock_gate, \
             patch("autoquant_etl.backfill.load_dimension_lookups") as mock_dims, \
             patch("autoquant_etl.backfill._month_has_full_data", return_value=False):

            # Mock VAHAN connector
            mock_vc = AsyncMock()
            mock_vc.extract.return_value = MagicMock(
                records=[MagicMock(registration_count=100)] * 100
            )
            MockVC.return_value.__aenter__ = AsyncMock(return_value=mock_vc)
            MockVC.return_value.__aexit__ = AsyncMock(return_value=False)

            # Mock gate pass
            mock_gate.return_value = MagicMock(passed=True)
            mock_dims.return_value = MagicMock()

            result = await run_backfill(
                pool=mock_pool,
                settings=settings,
                from_month="2025-12",
                to_month="2025-12",
                dry_run=True,
                force=True,
            )

        assert result.months_processed == 1
        assert result.records_loaded == 0  # dry run: no DB writes
