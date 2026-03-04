# AutoQuant — India Auto Registrations & Demand Dashboard

Private data platform for tracking Indian automobile registrations (VAHAN),
reconciling against industry body data (FADA), and generating demand-based
revenue proxies for listed auto OEMs.

> **Disclaimer:** All revenue figures are demand-based proxies using
> registrations × assumed ASPs. This is NOT accounting revenue.

## Architecture

```
VAHAN Dashboard ──→ Playwright Connector ──→ Validation Gate ──→ Transforms
                                                                    │
FADA Monthly PDFs ──→ PDF Connector ─────→ Reconciliation      ┌───┴───┐
                                                                ▼       ▼
                                                          Bronze    Silver
                                                                      │
                                                                      ▼
                                                                    Gold
                                                                      │
                                                            Next.js Dashboard
```

## Project Structure

```
autoquant/
├── .github/workflows/       # CI/CD pipelines
│   ├── etl-ci.yml           # ETL: lint + test + SQL validate
│   ├── frontend-ci.yml      # Frontend: lint + type-check + build
│   └── deploy-etl.yml       # ETL: Docker build + push to ghcr.io
├── db/                      # Database DDL & migrations
│   ├── 001_schema.sql       # Core schema (17 objects)
│   ├── 002_seed_dimensions_v2.sql  # OEM, segment, fuel, geo seeds
│   ├── 003_seed_asp.sql     # ASP assumption seeds
│   └── 004_monitoring_views.sql    # Monitoring views + heartbeat table
├── docs/
│   └── architecture.md      # System architecture (Mermaid diagram)
├── etl/                     # Python ETL pipeline
│   ├── autoquant_etl/
│   │   ├── connectors/      # VAHAN, FADA, (future: NAPIX)
│   │   ├── transforms/      # normalize, daily_delta, loader, reconcile, gold, asp_manager
│   │   ├── validators/      # Validation gate (7 checks)
│   │   ├── utils/           # database, alerts, logging, migrations, seeder, fy_calendar
│   │   ├── backfill.py      # Historical backfill orchestrator
│   │   ├── monitor.py       # Pipeline health monitor (6 checks)
│   │   ├── orchestrator.py  # Daily extraction pipeline
│   │   ├── config.py        # Pydantic settings from env
│   │   └── __main__.py      # CLI entry point (9 commands)
│   ├── tests/               # pytest test suite
│   ├── seeds/               # CSV seed data
│   ├── Dockerfile           # Playwright + Python 3.11
│   ├── docker-compose.yml   # Local dev
│   ├── requirements.txt
│   ├── pyproject.toml
│   ├── crontab              # 4 scheduled jobs
│   ├── railway.toml         # Railway deployment config
│   └── render.yaml          # Render deployment config
├── frontend/                # Next.js 14 dashboard
│   ├── app/                 # App Router pages + API routes
│   ├── components/          # UI components (charts, tables, cards)
│   ├── lib/                 # Supabase client, queries, formatters
│   ├── package.json
│   ├── vercel.json          # Vercel deployment config
│   └── tailwind.config.js
├── Makefile                 # Developer shortcuts
├── .gitignore
└── README.md
```

## ETL Commands

```bash
python -m autoquant_etl migrate           # Apply DB migrations
python -m autoquant_etl seed              # Seed dimension tables
python -m autoquant_etl health            # Check DB + connector health
python -m autoquant_etl status            # Full system status
python -m autoquant_etl extract-daily     # Daily VAHAN extraction
python -m autoquant_etl reconcile         # Monthly VAHAN vs FADA recon
python -m autoquant_etl estimate-revenue  # Quarterly revenue proxy
python -m autoquant_etl asp-calibrate     # Calibrate ASP from earnings
python -m autoquant_etl backfill          # Historical backfill
python -m autoquant_etl monitor           # Pipeline health check
```

## Scheduled Jobs

| Schedule            | Command          | Purpose                        |
|---------------------|------------------|--------------------------------|
| Daily 06:00 IST     | `extract-daily`  | VAHAN registration extraction  |
| Daily 13:00 IST     | `monitor --digest` | Health check + Telegram digest |
| Weekly Sun 02:00 IST | `backfill`      | Gap-filling trailing 6 months  |
| Monthly 5th 10:00 IST | `reconcile`   | VAHAN vs FADA reconciliation   |

## Tech Stack

| Layer      | Technology                                   |
|------------|----------------------------------------------|
| Database   | Supabase PostgreSQL                          |
| ETL        | Python 3.11, Playwright, asyncpg, structlog  |
| Frontend   | Next.js 14, Tailwind CSS, Recharts, Vercel   |
| Alerts     | Telegram Bot API                             |
| CI/CD      | GitHub Actions, ghcr.io, Railway/Render      |

## Setup

### ETL

```bash
cd etl
cp .env.example .env   # Fill in DATABASE_URL, Telegram keys
pip install -r requirements.txt
playwright install chromium
python -m autoquant_etl migrate
python -m autoquant_etl seed
python -m autoquant_etl health
```

### Frontend

```bash
cd frontend
cp .env.local.example .env.local  # Fill in NEXT_PUBLIC_SUPABASE_URL, keys
npm install
npm run dev
```

## License

Proprietary — Tusk Invest Research.
