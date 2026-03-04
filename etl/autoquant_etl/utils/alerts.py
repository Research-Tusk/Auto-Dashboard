"""
AutoQuant ETL — Telegram Alert Sender
=======================================
Sends structured alerts to a Telegram bot.

Used for:
  - Pipeline failures (extraction, validation, load)
  - Unmapped maker names (new OEMs or VAHAN name changes)
  - Daily health digest
  - Reconciliation anomalies

Alert types:
  - FAILURE: ❌ critical failure, immediate action needed
  - WARNING: ⚠️ non-critical, should be reviewed
  - INFO: ℹ️ informational digest

If Telegram credentials are not configured, alerts are logged only.
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from enum import Enum
from typing import Optional

import httpx
import structlog

from autoquant_etl.config import Settings

logger = structlog.get_logger(__name__)

TELEGRAM_API_BASE = "https://api.telegram.org/bot{token}/sendMessage"
MAX_MESSAGE_LENGTH = 4096  # Telegram's limit
RETRY_ATTEMPTS = 3
RETRY_BACKOFF_SECONDS = 2


class AlertLevel(str, Enum):
    FAILURE = "FAILURE"
    WARNING = "WARNING"
    INFO = "INFO"


@dataclass
class AlertResult:
    sent: bool
    message_id: Optional[int] = None
    error: Optional[str] = None


def _truncate_message(message: str, max_len: int = MAX_MESSAGE_LENGTH) -> str:
    """Truncate message if it exceeds Telegram's limit."""
    if len(message) <= max_len:
        return message
    truncation_note = "\n[...message truncated...]"
    return message[: max_len - len(truncation_note)] + truncation_note


def _build_alert_message(
    message: str,
    level: AlertLevel = AlertLevel.INFO,
    source: str = "AutoQuant ETL",
) -> str:
    """Build formatted alert message."""
    icons = {
        AlertLevel.FAILURE: "❌",
        AlertLevel.WARNING: "⚠️",
        AlertLevel.INFO: "ℹ️",
    }
    icon = icons.get(level, "ℹ️")
    header = f"*{icon} {source}*"
    return f"{header}\n\n{message}"


async def send_telegram_alert(
    settings: Settings,
    message: str,
    level: AlertLevel = AlertLevel.INFO,
    source: str = "AutoQuant ETL",
    parse_mode: str = "Markdown",
) -> AlertResult:
    """
    Send an alert message to the configured Telegram chat.

    If TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID are not configured,
    the message is logged at WARNING level instead.

    Args:
        settings: Application settings with Telegram credentials
        message: Alert message content
        level: Alert severity level
        source: Source label for the message header
        parse_mode: Telegram parse mode ('Markdown' or 'HTML')

    Returns:
        AlertResult indicating success or failure
    """
    if not settings.telegram_bot_token or not settings.telegram_chat_id:
        logger.warning(
            "alerts.telegram_not_configured",
            message=message[:200],
            level=level.value,
        )
        return AlertResult(sent=False, error="Telegram not configured")

    formatted_message = _build_alert_message(message, level=level, source=source)
    formatted_message = _truncate_message(formatted_message)

    url = TELEGRAM_API_BASE.format(token=settings.telegram_bot_token)
    payload = {
        "chat_id": settings.telegram_chat_id,
        "text": formatted_message,
        "parse_mode": parse_mode,
        "disable_web_page_preview": True,
    }

    for attempt in range(1, RETRY_ATTEMPTS + 1):
        try:
            async with httpx.AsyncClient(timeout=15) as client:
                resp = await client.post(url, json=payload)
                resp.raise_for_status()

            data = resp.json()
            if data.get("ok"):
                message_id = data.get("result", {}).get("message_id")
                logger.info(
                    "alerts.sent",
                    level=level.value,
                    message_id=message_id,
                )
                return AlertResult(sent=True, message_id=message_id)
            else:
                error_desc = data.get("description", "Unknown Telegram error")
                logger.warning("alerts.telegram_api_error", error=error_desc)
                return AlertResult(sent=False, error=error_desc)

        except httpx.HTTPStatusError as exc:
            error_msg = f"HTTP {exc.response.status_code}: {exc.response.text[:100]}"
            logger.warning("alerts.http_error", attempt=attempt, error=error_msg)
            if attempt == RETRY_ATTEMPTS:
                return AlertResult(sent=False, error=error_msg)
            await asyncio.sleep(RETRY_BACKOFF_SECONDS * attempt)

        except Exception as exc:
            error_msg = str(exc)
            logger.warning("alerts.send_error", attempt=attempt, error=error_msg)
            if attempt == RETRY_ATTEMPTS:
                return AlertResult(sent=False, error=error_msg)
            await asyncio.sleep(RETRY_BACKOFF_SECONDS * attempt)

    return AlertResult(sent=False, error="Max retries exceeded")


async def send_pipeline_failure(
    settings: Settings,
    pipeline_name: str,
    error: str,
    period: Optional[str] = None,
) -> AlertResult:
    """
    Send a pipeline failure alert.

    Args:
        settings: Application settings
        pipeline_name: Name of the failed pipeline step
        error: Error message or exception string
        period: Data period that failed (optional)

    Returns:
        AlertResult
    """
    period_str = f" ({period})" if period else ""
    message = f"Pipeline step `{pipeline_name}`{period_str} failed:\n```\n{error}\n```"
    return await send_telegram_alert(
        settings=settings,
        message=message,
        level=AlertLevel.FAILURE,
    )


async def send_unmapped_makers_alert(
    settings: Settings,
    unmapped_makers: list[str],
    period: Optional[str] = None,
) -> AlertResult:
    """
    Send alert for unmapped VAHAN maker names.

    Args:
        settings: Application settings
        unmapped_makers: List of unmapped maker name strings
        period: Data period

    Returns:
        AlertResult
    """
    period_str = f" for {period}" if period else ""
    makers_list = "\n".join(f"  - {m}" for m in sorted(unmapped_makers))
    message = (
        f"Unmapped VAHAN maker names{period_str}:\n{makers_list}\n"
        f"Add aliases to `dim_oem_alias` to resolve."
    )
    return await send_telegram_alert(
        settings=settings,
        message=message,
        level=AlertLevel.WARNING,
        source="AutoQuant ETL — Mapping",
    )


async def send_validation_failure(
    settings: Settings,
    failed_checks: list[str],
    period: Optional[str] = None,
) -> AlertResult:
    """
    Send QA gate validation failure alert.

    Args:
        settings: Application settings
        failed_checks: List of failed check names
        period: Data period

    Returns:
        AlertResult
    """
    period_str = f" for {period}" if period else ""
    checks_list = "\n".join(f"  - {c}" for c in failed_checks)
    message = f"Validation gate failed{period_str}:\n{checks_list}"
    return await send_telegram_alert(
        settings=settings,
        message=message,
        level=AlertLevel.FAILURE,
        source="AutoQuant ETL — QA Gate",
    )
