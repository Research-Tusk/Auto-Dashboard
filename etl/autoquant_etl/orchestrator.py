"""
AutoQuant ETL — Orchestrator
==============================
End-to-end daily extraction pipeline:

  1. Create extraction log entry
  2. Extract from VAHAN (Maker×Fuel + Maker×Category)
  3. Validate through QA gate
  4. Normalize to dimension IDs
  5. Compute daily delta (MTD → daily)
  6. Load bronze + silver
  7. Refresh materialized view
  8. Mark run as SUCCESS or FAILED
  9. Send Telegram alert on failure
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Optional

import asyncpg
import structlog

from autoquant_etl.config import Settings
from autoquant_etl.connectors.vahan import VahanConnector
from autoquant_etl.connectors.base import ExtractParams
from autoquant_etl.transforms.normalize import normalize_records, load_dimension_lookups
from autoquant_etl.transforms.daily_delta import compute_daily_delta
from autoquant_etl.transforms.loader import load_to_bronze, load_to_silver, refresh_mv
from autoquant_etl.validators.gate import run_validation_gate, GateConfig
from autoquant_etl.utils.alerts import send_telegram_alert

logger = structlog.get_logger(__name__)


@dataclass
class PipelineResult:
    success: bool
    records_extracted: int = 0
    records_loaded: int = 0
    run_id: Optional[int] = None
    error: Optional[str] = None


async def run_daily_pipeline(
    pool: asyncpg.Pool,
    settings: Settings,
    dry_run: bool = False,
    verbose: bool = False,
    target_date: Optional[date] = None,
) -> PipelineResult:
    """
    Run the end-to-end daily VAHAN extraction pipeline.

    Args:
        pool: asyncpg connection pool
        settings: application settings
        dry_run: if True, run extraction + validation but skip DB writes
        verbose: enable verbose structured logging
        target_date: date to extract for (defaults to yesterday)

    Returns:
        PipelineResult with outcome and counts
    """
    extract_date = target_date or (date.today() - timedelta(days=1))
    period = extract_date.strftime("%Y-%m")

    log = logger.bind(
        extract_date=extract_date.isoformat(),
        period=period,
        dry_run=dry_run,
    )
    log.info("pipeline.start")

    run_id: Optional[int] = None

    try:
        # 1. Create extraction log entry
        if not dry_run:
            async with pool.acquire() as conn:
                run_id = await conn.fetchval(
                    """
                    INSERT INTO raw_extraction_log (source, status)
                    VALUES ('VAHAN', 'RUNNING')
                    RETURNING run_id
                    """
                )

        # 2. Extract from VAHAN
        log.info("pipeline.extract_start")
        async with VahanConnector(settings) as vc:
            params = ExtractParams(
                period=period,
                period_type="month",
                state="All India",
                y_axis_types=["makerName", "vehicleClass"],
            )
            extraction_result = await vc.extract(params)

        records_extracted = len(extraction_result.records)
        log.info("pipeline.extract_done", records=records_extracted)

        if records_extracted == 0:
            raise ValueError("No records extracted from VAHAN")

        # 3. Validate
        log.info("pipeline.validate_start")
        gate_result = await run_validation_gate(
            records=extraction_result.records,
            pool=pool,
            month=period,
            config=GateConfig(),
        )
        if not gate_result.passed:
            raise ValueError(
                f"Validation gate failed: {', '.join(gate_result.failed_check_names)}"
            )
        log.info("pipeline.validate_pass")

        if dry_run:
            log.info("pipeline.dry_run_complete", records=records_extracted)
            return PipelineResult(success=True, records_extracted=records_extracted)

        # 4. Normalize
        log.info("pipeline.normalize_start")
        dims = await load_dimension_lookups(pool)
        normalized = normalize_records(
            records=extraction_result.records,
            dims=dims,
            alert_unmapped=True,
            settings=settings,
        )

        # 5. Compute daily delta
        log.info("pipeline.delta_start")
        delta_records = compute_daily_delta(
            normalized_records=normalized.records,
            month_date=extract_date.replace(day=1),
        )

        # 6. Load bronze + silver
        log.info("pipeline.load_start")
        await load_to_bronze(
            pool=pool,
            run_id=run_id,
            records=extraction_result.records,
            period=period,
        )
        records_loaded = await load_to_silver(
            pool=pool,
            run_id=run_id,
            daily_records=delta_records,
            month_key=extract_date.replace(day=1),
        )
        log.info("pipeline.load_done", loaded=records_loaded)

        # 7. Refresh materialized view
        await refresh_mv(pool)
        log.info("pipeline.mv_refreshed")

        # 8. Mark run as SUCCESS
        async with pool.acquire() as conn:
            await conn.execute(
                """
                UPDATE raw_extraction_log
                SET status='SUCCESS', completed_at=NOW(),
                    records_extracted=$1, records_loaded=$2
                WHERE run_id=$3
                """,
                records_extracted,
                records_loaded,
                run_id,
            )

        log.info("pipeline.success", extracted=records_extracted, loaded=records_loaded)
        return PipelineResult(
            success=True,
            records_extracted=records_extracted,
            records_loaded=records_loaded,
            run_id=run_id,
        )

    except Exception as exc:
        log.error("pipeline.failed", error=str(exc))

        # Mark run as FAILED
        if run_id is not None:
            try:
                async with pool.acquire() as conn:
                    await conn.execute(
                        """
                        UPDATE raw_extraction_log
                        SET status='FAILED', completed_at=NOW(), error_message=$1
                        WHERE run_id=$2
                        """,
                        str(exc),
                        run_id,
                    )
            except Exception as db_exc:
                log.error("pipeline.log_update_failed", error=str(db_exc))

        # Send Telegram alert
        await send_telegram_alert(
            settings=settings,
            message=f"❌ AutoQuant VAHAN extraction failed ({period}):\n{exc}",
        )

        return PipelineResult(success=False, run_id=run_id, error=str(exc))
