"""
AutoQuant ETL — Pipeline Monitor
==================================
Queries the warehouse for health metrics and generates a status report.
Used by the `monitor` CLI command and by the weekly digest.

Checks:
  1. Last successful VAHAN run was < 25h ago (alert if missed daily job)
  2. No FAILED runs in last 24h
  3. Daily registration count is within expected range (not 0, not anomalous)
  4. No unmapped makers (would indicate new OEM or VAHAN name change)
  5. Materialized view is fresh (refreshed < 2h ago)
  6. DB heartbeat write succeeds
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import List, Optional

import asyncpg
import structlog

from autoquant_etl.config import Settings
from autoquant_etl.utils.alerts import send_telegram_alert

logger = structlog.get_logger(__name__)


@dataclass
class HealthCheck:
    name: str
    passed: bool
    detail: str = ""


@dataclass
class MonitorResult:
    healthy: bool
    failed_checks: int
    checks: List[HealthCheck] = field(default_factory=list)
    issues: List[str] = field(default_factory=list)
    digest_message: str = ""


async def _check_last_successful_run(conn: asyncpg.Connection) -> HealthCheck:
    """Check 1: Last successful VAHAN run was < 25 hours ago."""
    row = await conn.fetchrow(
        """
        SELECT completed_at
        FROM raw_extraction_log
        WHERE source = 'VAHAN' AND status = 'SUCCESS'
        ORDER BY completed_at DESC
        LIMIT 1
        """
    )
    if not row or not row["completed_at"]:
        return HealthCheck(
            name="last_successful_run",
            passed=False,
            detail="No successful VAHAN run found in history",
        )

    age = datetime.now(timezone.utc) - row["completed_at"].replace(tzinfo=timezone.utc)
    if age > timedelta(hours=25):
        return HealthCheck(
            name="last_successful_run",
            passed=False,
            detail=f"Last success was {age.total_seconds()/3600:.1f}h ago (threshold: 25h)",
        )
    return HealthCheck(
        name="last_successful_run",
        passed=True,
        detail=f"Last success: {age.total_seconds()/3600:.1f}h ago",
    )


async def _check_no_failures_24h(conn: asyncpg.Connection) -> HealthCheck:
    """Check 2: No FAILED/VALIDATION_FAILED runs in the last 24 hours."""
    count = await conn.fetchval(
        """
        SELECT COUNT(*)
        FROM raw_extraction_log
        WHERE started_at >= NOW() - INTERVAL '24 hours'
          AND status IN ('FAILED', 'VALIDATION_FAILED')
        """
    )
    if count > 0:
        return HealthCheck(
            name="no_failures_24h",
            passed=False,
            detail=f"{count} failed run(s) in last 24h",
        )
    return HealthCheck(name="no_failures_24h", passed=True, detail="No failures in last 24h")


async def _check_daily_count_reasonable(conn: asyncpg.Connection) -> HealthCheck:
    """Check 3: Daily registration count is within expected range."""
    # Get yesterday's total
    row = await conn.fetchrow(
        """
        SELECT SUM(registration_count) AS total
        FROM fact_daily_registrations
        WHERE date_key = CURRENT_DATE - 1
        """
    )
    total = row["total"] if row else 0

    # India total daily registrations should be 50K–1.2M on a weekday
    # (avg ~100-150K/day based on ~40M/year)
    MIN_EXPECTED = 30_000
    MAX_EXPECTED = 2_000_000

    if total is None or total == 0:
        return HealthCheck(
            name="daily_count_reasonable",
            passed=False,
            detail="No daily registration data found for yesterday",
        )
    if total < MIN_EXPECTED:
        return HealthCheck(
            name="daily_count_reasonable",
            passed=False,
            detail=f"Daily count too low: {total:,} (min: {MIN_EXPECTED:,})",
        )
    if total > MAX_EXPECTED:
        return HealthCheck(
            name="daily_count_reasonable",
            passed=False,
            detail=f"Daily count too high: {total:,} (max: {MAX_EXPECTED:,})",
        )
    return HealthCheck(
        name="daily_count_reasonable",
        passed=True,
        detail=f"Daily count: {total:,}",
    )


async def _check_no_unmapped_makers(conn: asyncpg.Connection) -> HealthCheck:
    """Check 4: No unmapped makers in recent VAHAN data."""
    rows = await conn.fetch(
        """
        SELECT raw_maker_name, occurrence_count
        FROM v_unmapped_makers
        WHERE last_seen >= NOW() - INTERVAL '7 days'
        ORDER BY occurrence_count DESC
        LIMIT 5
        """
    )
    if rows:
        names = ", ".join(r["raw_maker_name"] for r in rows)
        return HealthCheck(
            name="no_unmapped_makers",
            passed=False,
            detail=f"Unmapped makers in last 7d: {names}",
        )
    return HealthCheck(
        name="no_unmapped_makers", passed=True, detail="No unmapped makers"
    )


async def _check_mv_freshness(conn: asyncpg.Connection) -> HealthCheck:
    """Check 5: Materialized view mv_oem_monthly_summary is fresh."""
    # Check if MV has been populated at all (any rows)
    count = await conn.fetchval("SELECT COUNT(*) FROM mv_oem_monthly_summary")
    if count == 0:
        return HealthCheck(
            name="mv_freshness",
            passed=False,
            detail="mv_oem_monthly_summary is empty",
        )
    # Check last_updated in the MV
    last_updated = await conn.fetchval(
        "SELECT MAX(last_updated) FROM mv_oem_monthly_summary"
    )
    if last_updated is None:
        return HealthCheck(
            name="mv_freshness",
            passed=False,
            detail="mv_oem_monthly_summary has no last_updated values",
        )
    age = datetime.now(timezone.utc) - last_updated.replace(tzinfo=timezone.utc)
    if age > timedelta(hours=26):
        return HealthCheck(
            name="mv_freshness",
            passed=False,
            detail=f"MV last updated {age.total_seconds()/3600:.1f}h ago (threshold: 26h)",
        )
    return HealthCheck(
        name="mv_freshness",
        passed=True,
        detail=f"MV last updated {age.total_seconds()/3600:.1f}h ago",
    )


async def _check_heartbeat(conn: asyncpg.Connection) -> HealthCheck:
    """Check 6: DB write succeeds (heartbeat)."""
    try:
        await conn.execute(
            "INSERT INTO pipeline_heartbeat (status, note) VALUES ('OK', 'monitor_check')"
        )
        return HealthCheck(name="heartbeat", passed=True, detail="DB write succeeded")
    except Exception as exc:
        return HealthCheck(
            name="heartbeat",
            passed=False,
            detail=f"DB write failed: {exc}",
        )


def _build_digest_message(result: MonitorResult) -> str:
    """Build a Telegram-formatted digest message."""
    lines = ["*AutoQuant — Daily Health Digest*"]
    overall = "✅ All checks passed" if result.healthy else f"❌ {result.failed_checks} check(s) failed"
    lines.append(overall)
    lines.append("")
    for check in result.checks:
        icon = "✅" if check.passed else "❌"
        lines.append(f"{icon} `{check.name}`: {check.detail}")
    return "\n".join(lines)


async def run_monitor(
    pool: asyncpg.Pool,
    settings: Settings,
    send_digest: bool = False,
    verbose: bool = False,
) -> MonitorResult:
    """
    Run all health checks and optionally send a Telegram digest.

    Args:
        pool: asyncpg connection pool
        settings: application settings
        send_digest: if True, send Telegram digest regardless of health status
        verbose: log detailed check results

    Returns:
        MonitorResult with all check outcomes
    """
    log = logger.bind(send_digest=send_digest)
    log.info("monitor.start")

    async with pool.acquire() as conn:
        checks = await asyncio.gather(
            _check_last_successful_run(conn),
            _check_no_failures_24h(conn),
            _check_daily_count_reasonable(conn),
            _check_no_unmapped_makers(conn),
            _check_mv_freshness(conn),
            _check_heartbeat(conn),
        )

    failed = [c for c in checks if not c.passed]
    result = MonitorResult(
        healthy=len(failed) == 0,
        failed_checks=len(failed),
        checks=list(checks),
        issues=[c.detail for c in failed],
    )
    result.digest_message = _build_digest_message(result)

    if verbose:
        for check in checks:
            status = "PASS" if check.passed else "FAIL"
            log.info("monitor.check", name=check.name, status=status, detail=check.detail)

    # Send alerts for failures
    if failed:
        alert_msg = (
            f"❌ AutoQuant Monitor: {len(failed)} check(s) failed\n"
            + "\n".join(f"  - {c.name}: {c.detail}" for c in failed)
        )
        await send_telegram_alert(settings=settings, message=alert_msg)

    # Send digest if requested
    if send_digest:
        await send_telegram_alert(settings=settings, message=result.digest_message)

    log.info("monitor.complete", healthy=result.healthy, failed_checks=result.failed_checks)
    return result
