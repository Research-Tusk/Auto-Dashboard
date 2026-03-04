"""
Tests for the pipeline monitor.
"""

from __future__ import annotations

import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from datetime import datetime, timezone, timedelta

from autoquant_etl.monitor import run_monitor, MonitorResult


class TestRunMonitor:
    @pytest.mark.asyncio
    async def test_all_checks_pass(self, mock_pool, settings):
        """Monitor should return healthy when all checks pass."""
        with patch("autoquant_etl.monitor._check_last_successful_run") as c1, \
             patch("autoquant_etl.monitor._check_no_failures_24h") as c2, \
             patch("autoquant_etl.monitor._check_daily_count_reasonable") as c3, \
             patch("autoquant_etl.monitor._check_no_unmapped_makers") as c4, \
             patch("autoquant_etl.monitor._check_mv_freshness") as c5, \
             patch("autoquant_etl.monitor._check_heartbeat") as c6:

            for mock_check in [c1, c2, c3, c4, c5, c6]:
                mock_check.return_value = MagicMock(passed=True, name="check", detail="ok")

            result = await run_monitor(
                pool=mock_pool,
                settings=settings,
                send_digest=False,
            )

        assert result.healthy
        assert result.failed_checks == 0

    @pytest.mark.asyncio
    async def test_one_check_fails(self, mock_pool, settings):
        """Monitor should return unhealthy when any check fails."""
        with patch("autoquant_etl.monitor._check_last_successful_run") as c1, \
             patch("autoquant_etl.monitor._check_no_failures_24h") as c2, \
             patch("autoquant_etl.monitor._check_daily_count_reasonable") as c3, \
             patch("autoquant_etl.monitor._check_no_unmapped_makers") as c4, \
             patch("autoquant_etl.monitor._check_mv_freshness") as c5, \
             patch("autoquant_etl.monitor._check_heartbeat") as c6:

            c1.return_value = MagicMock(passed=False, name="last_run", detail="26h ago")
            for mock_check in [c2, c3, c4, c5, c6]:
                mock_check.return_value = MagicMock(passed=True, name="check", detail="ok")

            result = await run_monitor(
                pool=mock_pool,
                settings=settings,
                send_digest=False,
            )

        assert not result.healthy
        assert result.failed_checks == 1
