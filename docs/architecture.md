# AutoQuant — System Architecture

## Architecture Diagram (Mermaid.js)

```mermaid
graph TB
    subgraph "Data Sources"
        VAHAN["\ud83c\udfdb\ufe0f VAHAN Dashboard<br/>(vahan.parivahan.gov.in)<br/>JSF / Headless Browser"]
        FADA["\ud83d\udcc4 FADA Monthly PDFs<br/>(fada.in)"]
        BSE["\ud83d\udcca OEM BSE Filings<br/>(Monthly Wholesale)"]
        NAPIX["\ud83d\udd17 NAPIX API<br/>(Future Official Feed)"]:::future
        VONTER["\ud83d\udcc1 Vonter/india-vehicle-stats<br/>(GitHub Reference CSVs)"]:::reference
    end

    subgraph "ETL Server (Railway / Render)"
        direction TB
        CONN["Connector / Adapter Layer"]
        VC["VAHAN Connector<br/>(Playwright)"]
        FC["FADA Connector<br/>(pdfplumber + tabula)"]
        WC["Wholesale Connector<br/>(BSE Scraper)"]
        NC["NAPIX Connector<br/>(REST API Client)"]:::future

        CONN --- VC
        CONN --- FC
        CONN --- WC
        CONN -.- NC

        VALID["Validation Gate<br/>\u2022 Row count check<br/>\u2022 Maker mapping check<br/>\u2022 Fuel mapping check<br/>\u2022 Z-score anomaly<br/>\u2022 Negative delta handler"]
        TRANSFORM["Transform Pipeline<br/>Raw \u2192 Silver \u2192 Gold<br/>\u2022 Normalize makers \u2192 dim_oem<br/>\u2022 Map vehicle class \u2192 segment<br/>\u2022 Map fuel \u2192 powertrain bucket<br/>\u2022 Compute daily deltas<br/>\u2022 Aggregate monthly"]
        SCHED["Scheduler<br/>\u2022 Daily 6 AM IST (VAHAN)<br/>\u2022 Weekly Sun 2 AM (Backfill)<br/>\u2022 Monthly 5th 10 AM (Recon)"]
        ALERT["Telegram Alert Bot<br/>\u2022 Failures<br/>\u2022 Unmapped makers<br/>\u2022 Anomalies<br/>\u2022 QA gate failures"]

        VC --> VALID
        FC --> VALID
        WC --> VALID
        VALID --> TRANSFORM
        SCHED -.-> VC
        SCHED -.-> FC
        SCHED -.-> WC
        VALID --> ALERT
    end

    subgraph "Database (Supabase / Neon PostgreSQL)"
        direction TB
        BRONZE["\ud83e\udd49 Bronze Layer<br/>\u2022 raw_extraction_log<br/>\u2022 raw_vahan_snapshot<br/>\u2022 raw_fada_monthly<br/>\u2022 raw_oem_wholesale"]
        DIMS["\ud83d\udcd0 Dimension Tables<br/>\u2022 dim_date<br/>\u2022 dim_oem + dim_oem_alias<br/>\u2022 dim_segment<br/>\u2022 dim_fuel<br/>\u2022 dim_vehicle_class_map<br/>\u2022 dim_geo"]
        SILVER["\ud83e\udd48 Silver Layer<br/>\u2022 fact_daily_registrations<br/>\u2022 fact_monthly_registrations<br/>\u2022 fact_monthly_wholesale"]
        GOLD["\ud83e\udd47 Gold Layer<br/>\u2022 fact_asp_master<br/>\u2022 est_quarterly_revenue<br/>\u2022 mv_oem_monthly_summary (MV)"]
        BRONZE --> SILVER
        SILVER --> GOLD
    end

    subgraph "Frontend (Vercel)"
        direction TB
        API["Next.js API Routes<br/>(Read-only from DB)"]
        PAGES["Dashboard Pages<br/>\u2022 /dashboard (Industry Pulse)<br/>\u2022 /oem/[ticker] (OEM Deep Dive)<br/>\u2022 /revenue (Revenue Estimator)<br/>\u2022 /scorecard (Quarterly Scorecard)<br/>\u2022 /history (Historical Explorer)"]
        ISR["ISR / SSR<br/>\u2022 ISR for historical (1hr)<br/>\u2022 SSR for live/current data"]
        UI["UI Layer<br/>shadcn/ui + Recharts"]

        API --> PAGES
        PAGES --> ISR
        ISR --> UI
    end

    %% Data Flow Arrows
    VAHAN --> VC
    FADA --> FC
    BSE --> WC
    NAPIX -.-> NC
    VONTER -.-> VC

    TRANSFORM --> BRONZE
    TRANSFORM --> SILVER
    TRANSFORM --> GOLD

    GOLD --> API
    SILVER --> API
    DIMS --> API

    TRANSFORM -.->|"Revalidation Webhook"| ISR

    %% Styling
    classDef future stroke-dasharray: 5 5, stroke:#888, fill:#f9f9f9
    classDef reference stroke-dasharray: 3 3, stroke:#aaa, fill:#fafafa
```

## Data Flow Summary

```
VAHAN Dashboard ──→ Playwright Connector ──→ Validation Gate ──→ Transform Pipeline
                                                    │                     │
                                              (FAIL → Telegram)     ┌─────┴─────┐
                                                                    ▼           ▼
                                                              Bronze Tables  Silver Tables
                                                                               │
                                                                               ▼
                                                                         Gold Tables
                                                                               │
                                                                               ▼
                                                                   Materialized Views
                                                                               │
                                                              (ISR Revalidation Webhook)
                                                                               │
                                                                               ▼
                                                                    Next.js API Routes
                                                                               │
                                                                               ▼
                                                                    Vercel Dashboard UI
```

## Connector / Adapter Pattern

All data source connectors implement a common `BaseConnector` interface:

```
┌─────────────────────────────────────────────────────┐
│                 BaseConnector (ABC)                   │
│                                                       │
│  + extract(params: ExtractParams) → RawData           │
│  + validate(data: RawData) → ValidationResult         │
│  + get_source_name() → str                            │
│  + health_check() → bool                              │
└───────────────────────────┬───────────────────────────┘
            ┌───────────────┼───────────────┬──────────────────┐
            ▼               ▼               ▼                  ▼
   VahanConnector    FadaConnector   WholesaleConnector   NapixConnector
   (Playwright)      (pdfplumber)    (BSE Scraper)        (REST API)
                                                          [Future]
```

This pattern ensures VAHAN scraper can be swapped for NAPIX API feed
without touching warehouse, transforms, or frontend.
