"""
AutoQuant ETL — Telegram Alert Utilities
=========================================
Sends operational alerts and pipeline digests via the Telegram Bot API.
Gracefully degrades (log-only) when credentials are not configured.
"""

from __future__ import annotations

from typing import Optional

import httpx
import structlog

from autoquant_etl.config import Settings

logger = structlog.get_logger(__name__)

TELEGRAM_API_BASE = "https://api.telegram.org/bot{token}/sendMessage"


async def send_telegram_alert(settings: Settings, message: str) -> None:
    """
    Send a text message via the configured Telegram bot.

    If telegram_bot_token or telegram_chat_id is not set in Settings,
    the message is logged at WARNING level instead of raising an error.

    Args:
        settings: application Settings (reads telegram_bot_token / chat_id)
        message: plain text or Markdown-formatted message body
    """
    token = settings.telegram_bot_token
    chat_id = settings.telegram_chat_id

    if not token or not chat_id:
        logger.warning(
            "alerts.telegram_not_configured",
            message_preview=message[:120],
        )
        return

    url = TELEGRAM_API_BASE.format(token=token)
    payload = {
        "chat_id": chat_id,
        "text": message,
        "parse_mode": "Markdown",
        "disable_web_page_preview": True,
    }

    try:
        async with httpx.AsyncClient(timeout=15.0) as client:
            resp = await client.post(url, json=payload)
            resp.raise_for_status()
        logger.info("alerts.telegram_sent", chat_id=chat_id)
    except httpx.HTTPStatusError as exc:
        logger.error(
            "alerts.telegram_http_error",
            status=exc.response.status_code,
            body=exc.response.text[:200],
        )
    except Exception as exc:
        logger.error("alerts.telegram_failed", error=str(exc))


async def send_pipeline_digest(settings: Settings, pool) -> None:
    """
    Query v_pipeline_status and send a formatted summary via Telegram.

    Args:
        settings: application Settings
        pool: asyncpg connection pool (used to query v_pipeline_status)
    """
    try:
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT metric, value FROM v_pipeline_status ORDER BY metric"
            )
            freshness_rows = await conn.fetch(
                "SELECT source, last_success, failures_24h "
                "FROM v_data_freshness ORDER BY source"
            )
    except Exception as exc:
        logger.error("alerts.digest_query_failed", error=str(exc))
        return

    lines = ["*AutoQuant ETL — Pipeline Digest*", ""]

    if rows:
        lines.append("*Pipeline Status*")
        for row in rows:
            lines.append(f"  • {row['metric']}: `{row['value']}`")
        lines.append("")

    if freshness_rows:
        lines.append("*Data Freshness*")
        for row in freshness_rows:
            last = str(row["last_success"])[:16] if row["last_success"] else "never"
            failures = row["failures_24h"]
            status_icon = "✅" if failures == 0 else "⚠️"
            lines.append(
                f"  {status_icon} {row['source']}: last success `{last}`, "
                f"failures 24h: `{failures}`"
            )

    message = "\n".join(lines)
    await send_telegram_alert(settings=settings, message=message)
