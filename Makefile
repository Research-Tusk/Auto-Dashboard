# ===========================================================================
# AutoQuant — Developer Shortcuts
# ===========================================================================

.PHONY: help setup-etl setup-frontend lint test migrate seed health status \
        extract reconcile backfill monitor docker-build docker-run

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

# ---------------------------------------------------------------------------
# ETL
# ---------------------------------------------------------------------------

setup-etl: ## Install ETL dependencies + Playwright
	cd etl && pip install -r requirements.txt && playwright install chromium

lint: ## Run ruff linter on ETL code
	cd etl && ruff check autoquant_etl/ && ruff format --check autoquant_etl/

test: ## Run ETL tests with pytest
	cd etl && python -m pytest tests/ -v --tb=short

test-cov: ## Run ETL tests with coverage
	cd etl && python -m pytest tests/ -v --cov=autoquant_etl --cov-report=term-missing

migrate: ## Apply pending DB migrations
	cd etl && python -m autoquant_etl migrate

seed: ## Seed dimension tables
	cd etl && python -m autoquant_etl seed

health: ## Check DB + connector health
	cd etl && python -m autoquant_etl health

status: ## Show full system status
	cd etl && python -m autoquant_etl status

extract: ## Run daily VAHAN extraction (dry-run)
	cd etl && python -m autoquant_etl extract-daily --dry-run

reconcile: ## Run FADA reconciliation (requires --pdf-path or --pdf-url)
	cd etl && python -m autoquant_etl reconcile --help

backfill: ## Run historical backfill (dry-run, last 3 months)
	cd etl && python -m autoquant_etl backfill \
		--from-month "$$(date -d '-3 months' +%Y-%m)" \
		--to-month "$$(date +%Y-%m)" \
		--dry-run --verbose

monitor: ## Run pipeline health check
	cd etl && python -m autoquant_etl monitor --verbose

monitor-digest: ## Run health check + send Telegram digest
	cd etl && python -m autoquant_etl monitor --digest --verbose

# ---------------------------------------------------------------------------
# Frontend
# ---------------------------------------------------------------------------

setup-frontend: ## Install frontend dependencies
	cd frontend && npm install

dev: ## Start frontend dev server
	cd frontend && npm run dev

build-frontend: ## Build frontend for production
	cd frontend && npm run build

lint-frontend: ## Run ESLint + TypeScript check on frontend
	cd frontend && npm run lint && npm run type-check

# ---------------------------------------------------------------------------
# Docker
# ---------------------------------------------------------------------------

docker-build: ## Build ETL Docker image locally
	cd etl && docker build -t autoquant-etl:local .

docker-run: ## Run ETL Docker image (dry-run)
	cd etl && docker run --rm --env-file .env autoquant-etl:local \
		python -m autoquant_etl extract-daily --dry-run

docker-compose-up: ## Start local dev stack
	cd etl && docker compose up --build

# ---------------------------------------------------------------------------
# SQL
# ---------------------------------------------------------------------------

sql-apply: ## Apply all SQL migrations to local DB
	@echo "Applying schema..."
	cd db && psql "$$DATABASE_URL" -f 001_schema.sql
	cd db && psql "$$DATABASE_URL" -f 002_seed_dimensions_v2.sql
	cd db && psql "$$DATABASE_URL" -f 003_seed_asp.sql
	cd db && psql "$$DATABASE_URL" -f 004_monitoring_views.sql
	@echo "Done."
