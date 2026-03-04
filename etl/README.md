# AutoQuant ETL

Python 3.11 ETL pipeline for India automobile registration data.

## Commands

```bash
python -m autoquant_etl migrate           # Apply DB migrations
python -m autoquant_etl seed              # Seed dimension tables
python -m autoquant_etl health            # Check DB + connector health
python -m autoquant_etl status            # Full system status
python -m autoquant_etl extract-daily     # Daily VAHAN extraction
python -m autoquant_etl reconcile         # Monthly VAHAN vs FADA reconciliation
python -m autoquant_etl estimate-revenue  # Quarterly revenue proxy calculation
python -m autoquant_etl asp-calibrate     # Calibrate ASP from earnings data
python -m autoquant_etl backfill          # Historical backfill
python -m autoquant_etl monitor           # Pipeline health check
```

## Setup

```bash
cp .env.example .env
# Edit .env: set DATABASE_URL, TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID
pip install -r requirements.txt
playwright install chromium
python -m autoquant_etl migrate
python -m autoquant_etl seed
python -m autoquant_etl health
```

## Project Structure

```
autoquant_etl/
├── __init__.py          # Package init
├── __main__.py          # CLI entry point (typer)
├── config.py            # Pydantic settings (reads .env)
├── orchestrator.py      # Daily extraction orchestrator
├── backfill.py          # Historical backfill
├── monitor.py           # Pipeline health monitor
├── connectors/
│   ├── base.py            # Abstract BaseConnector
│   ├── vahan.py           # VAHAN Playwright connector
│   └── fada.py            # FADA PDF connector
├── transforms/
│   ├── normalize.py       # Maker/fuel/class normalization
│   ├── daily_delta.py     # Daily delta computation
│   ├── loader.py          # DB write layer
│   ├── reconcile.py       # VAHAN vs FADA reconciliation
│   ├── gold.py            # Gold layer aggregations
│   └── asp_manager.py     # ASP calibration
├── validators/
│   └── gate.py            # Validation gate (7 checks)
└── utils/
    ├── database.py        # asyncpg connection pool
    ├── alerts.py          # Telegram alert sender
    ├── logging.py         # structlog configuration
    ├── migrations.py      # DB migration runner
    ├── seeder.py          # Dimension table seeder
    └── fy_calendar.py     # FY quarter utilities
```

## Environment Variables

| Variable                | Required | Default  | Description                     |
|-------------------------|----------|----------|---------------------------------|
| `DATABASE_URL`          | Yes      | —        | PostgreSQL connection string     |
| `TELEGRAM_BOT_TOKEN`    | No       | —        | Telegram bot token for alerts   |
| `TELEGRAM_CHAT_ID`      | No       | —        | Telegram chat/channel ID        |
| `DRY_RUN`               | No       | `false`  | Skip DB writes if `true`        |
| `LOG_LEVEL`             | No       | `INFO`   | structlog log level             |
| `PLAYWRIGHT_HEADLESS`   | No       | `true`   | Run browser headless            |

## Deployment

### Railway

1. Connect repo to Railway
2. Set environment variables
3. Railway auto-detects `railway.toml` and builds/deploys
4. Cron jobs are configured via crontab or Railway's scheduler

### Render

1. Connect repo to Render
2. Set environment variables
3. Render auto-detects `render.yaml`
4. Background workers for scheduled jobs

## Tests

```bash
python -m pytest tests/ -v --tb=short
```

Tests use mocked DB and mock Playwright pages — no live VAHAN access needed.
