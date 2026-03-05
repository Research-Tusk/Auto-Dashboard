"""
AutoQuant ETL — Pipeline Health Monitor
==========================================
Runs a series of health checks against the pipeline state and optionally
sends a Telegram digest.

Checks performed:
  1. last_extraction_age  — most recent successful extraction < 36 hours ago
  2. no_recent_failures   — no FAILED runs in the last 24 hours
  3. unmapped_makers       — count of unmapped makers below threshold (10)
  4. heartbeat             — insert a heartbeat row to confirm monitor ran

If send_digest=True, queries v_pipeline_status and sends via Telegram.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import List, Optional

import asyncpg
import structlog

from autoquant_etl.config import Settings
from autoquant_etl.utils.alerts import send_pipeline_digest

logger = structlog.get_logger(__name__)

# Thresholds
MAX_EXTRACTION_AGE_HOURS = 36
MAX_RECENT_FAILURES_24H = 0
MAX_UNMAPPED_MAKERS = 10


@dataclass
class MonitorResult:
    """Result of a pipeline health monitor run."""
    healthy: bool
    failed_checks: int = 0
    issues: List[str] = field(default_factory=list)
    checks_run: int = 0


async def run_monitor(
    pool: asyncpg.Pool,
    settings: Settings,
    send_digest: bool = False,
    verbose: bool = False,
) -> MonitorResult:
    """
    Run pipeline health checks and optionally send Telegram digest.

    Args:
        pool: asyncpg connection pool
        settings: application settings
        send_digest: if True, send formatted digest via Telegram
        verbose: enable verbose logging

    Returns:
        MonitorResult with pass/fail status and issue list
    """
    issues: List[str] = []
    checks_run = 0

    log = logger.bind(send_digest=send_digest)
    log.info("monitor.start")

    async with pool.acquire() as conn:
        # -----------------------------------------------------------------
        # Check 1: Last successful extraction < 36 hours ago
        # -----------------------------------------------------------------
        checks_run += 1
        try:
            last_success_ts = await conn.fetchval(
                """
                SELECT MAX(completed_at)
                FROM raw_extraction_log
                WHERE status = 'SUCCESS'
                """
            )
            if last_success_ts is None:
                issues.append(
                    "No successful extractions found in raw_extraction_log"
                )
                logger.warning("monitor.check_failed", check="last_extraction_age", reason="no_records")
            else:
                # Make timezone-aware for comparison
                now_utc = datetime.now(timezone.utc)
                if last_success_ts.tzinfo is None:
                    last_success_ts = last_success_ts.replace(tzinfo=timezone.utc)
                age_hours = (now_utc - last_success_ts).total_seconds() / 3600

                if age_hours > MAX_EXTRACTION_AGE_HOURS:
                    issues.append(
                        f"Last successful extraction was {age_hours:.1f}h ago "
                        f"(threshold: {MAX_EXTRACTION_AGE_HOURS}h)"
                    )
                    logger.warning(
                        "monitor.check_failed",
                        check="last_extraction_age",
                        age_hours=round(age_hours, 1),
                    )
                else:
                    logger.debug(
                        "monitor.check_passed",
                        check="last_extraction_age",
                        age_hours=round(age_hours, 1),
                    )
        except Exception as exc:
            issues.append(f"last_extraction_age check error: {exc}")
            logger.error("monitor.check_error", check="last_extraction_age", error=str(exc))

        # -----------------------------------------------------------------
        # Check 2: No FAILED runs in the last 24 hours
        # -----------------------------------------------------------------
        checks_run += 1
        try:
            failed_24h: int = await conn.fetchval(
                """
                SELECT COUNT(*)
                FROM raw_extraction_log
                WHERE status = 'FAILED'
                  AND started_at >= NOW() - INTERVAL '24 hours'
                """
            )
            failed_24h = int(failed_24h or 0)

            if failed_24h > MAX_RECENT_FAILURES_24H:
                issues.append(
                    f"{failed_24h} FAILED extraction run(s) in the last 24 hours"
                )
                logger.warning(
                    "monitor.check_failed",
                    check="no_recent_failures",
                    failed_count=failed_24h,
                )
            else:
                logger.debug(
                    "monitor.check_passed",
                    check="no_recent_failures",
                    failed_24h=failed_24h,
                )
        except Exception as exc:
            issues.append(f"no_recent_failures check error: {exc}")
            logger.error("monitor.check_error", check="no_recent_failures", error=str(exc))

        # -----------------------------------------------------------------
        # Check 3: Unmapped makers below threshold
        # -----------------------------------------------------------------
        checks_run += 1
        try:
            unmapped_count: int = await conn.fetchval(
                "SELECT COUNT(*) FROM v_unmapped_makers"
            )
            unmapped_count = int(unmapped_count or 0)

            if unmapped_count > MAX_UNMAPPED_MAKERS:
                issues.append(
                    f"{unmapped_count} unmapped maker(s) detected "
                    f"(threshold: {MAX_UNMAPPED_MAKERS})"
                )
                logger.warning(
                    "monitor.check_failed",
                    check="unmapped_makers",
                    count=unmapped_count,
                )
            else:
                logger.debug(
                    "monitor.check_passed",
                    check="unmapped_makers",
                    count=unmapped_count,
                )
        except Exception as exc:
            issues.append(f"unmapped_makers check error: {exc}")
            logger.error("monitor.check_error", check="unmapped_makers", error=str(exc))

        # -----------------------------------------------------------------
        # Check 4: Insert heartbeat
        # -----------------------------------------------------------------
        healthy = len(issues) == 0
        try:
            await conn.execute(
                """
                INSERT INTO pipeline_heartbeat (component, status, details)
                VALUES ('monitor', $1, $2::jsonb)
                """,
                "OK" if healthy else "DEGRADED",
                f'{"issues": {len(issues)}, "checks_run": {checks_run}}',
            )
            logger.debug("monitor.heartbeat_inserted")
        except Exception as exc:
            logger.warning("monitor.heartbeat_failed", error=str(exc))

    failed_checks = len(issues)

    log.info(
        "monitor.complete",
        healthy=healthy,
        failed_checks=failed_checks,
        checks_run=checks_run,
    )

    if verbose and issues:
        for issue in issues:
            logger.info("monitor.issue", detail=issue)

    # Optionally send Telegram digest
    if send_digest:
        try:
            await send_pipeline_digest(settings=settings, pool=pool)
        except Exception as exc:
            logger.error("monitor.digest_failed", error=str(exc))

    return MonitorResult(
        healthy=healthy,
        failed_checks=failed_checks,
        issues=issues,
        checks_run=checks_run,
    )
