-- ============================================================================
-- AutoQuant: India Auto Registrations & Demand Dashboard
-- Complete PostgreSQL DDL
-- Version: 1.0.0
-- Date: 2026-03-04
-- ============================================================================

-- Enable required extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- ============================================================================
-- DIMENSION TABLES
-- ============================================================================

-- ---------------------------------------------------------------------------
-- dim_date: Calendar dimension (2016-01-01 → 2027-12-31)
-- ---------------------------------------------------------------------------
CREATE TABLE dim_date (
    date_key         DATE        PRIMARY KEY,
    calendar_year    SMALLINT    NOT NULL,
    calendar_month   SMALLINT    NOT NULL CHECK (calendar_month BETWEEN 1 AND 12),
    calendar_quarter SMALLINT    NOT NULL CHECK (calendar_quarter BETWEEN 1 AND 4),
    fy_year          VARCHAR(6)  NOT NULL,   -- e.g. 'FY26' (Apr 2025 – Mar 2026)
    fy_quarter       VARCHAR(8)  NOT NULL,   -- e.g. 'Q3FY26'
    fy_quarter_num   SMALLINT    NOT NULL CHECK (fy_quarter_num BETWEEN 1 AND 4),
    month_name       VARCHAR(10) NOT NULL,
    day_of_week      SMALLINT    NOT NULL CHECK (day_of_week BETWEEN 0 AND 6), -- 0=Sun
    is_weekend       BOOLEAN     NOT NULL
);

CREATE INDEX idx_dim_date_fy_year ON dim_date (fy_year);
CREATE INDEX idx_dim_date_fy_quarter ON dim_date (fy_quarter);
CREATE INDEX idx_dim_date_calendar_ym ON dim_date (calendar_year, calendar_month);

COMMENT ON TABLE dim_date IS 'Calendar dimension. FY follows India convention: Apr–Mar. FY26 = Apr 2025 – Mar 2026.';

-- ---------------------------------------------------------------------------
-- dim_oem: Listed and tracked OEMs
-- ---------------------------------------------------------------------------
CREATE TABLE dim_oem (
    oem_id           SERIAL      PRIMARY KEY,
    oem_name         VARCHAR(100) NOT NULL UNIQUE,
    nse_ticker       VARCHAR(20),
    bse_code         VARCHAR(10),
    is_listed        BOOLEAN     NOT NULL DEFAULT FALSE,
    is_in_scope      BOOLEAN     NOT NULL DEFAULT TRUE,
    primary_segments TEXT[],     -- e.g. '{PV,CV}' or '{2W}'
    created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_dim_oem_ticker ON dim_oem (nse_ticker) WHERE nse_ticker IS NOT NULL;

COMMENT ON TABLE dim_oem IS 'Master OEM dimension. is_in_scope controls inclusion in financial panels. Unlisted OEMs tracked for TIV reconciliation.';

-- ---------------------------------------------------------------------------
-- dim_oem_alias: Maps source-specific maker names to canonical OEM
-- ---------------------------------------------------------------------------
CREATE TABLE dim_oem_alias (
    alias_id    SERIAL       PRIMARY KEY,
    oem_id      INT          NOT NULL REFERENCES dim_oem(oem_id) ON DELETE CASCADE,
    source      VARCHAR(20)  NOT NULL CHECK (source IN ('VAHAN', 'FADA', 'BSE', 'SIAM', 'OTHER')),
    alias_name  VARCHAR(150) NOT NULL,
    is_active   BOOLEAN      NOT NULL DEFAULT TRUE,
    created_at  TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    UNIQUE (source, alias_name)
);

CREATE INDEX idx_dim_oem_alias_lookup ON dim_oem_alias (alias_name, source) WHERE is_active = TRUE;

COMMENT ON TABLE dim_oem_alias IS 'Maps raw maker names from each source to canonical dim_oem. Any unmapped name triggers a Telegram alert. SPECIAL RULE: TATA MOTORS LTD in VAHAN is disambiguated by vehicle_class at transform layer (PV classes → Tata PV oem_id, CV classes → Tata CV oem_id).';

-- ---------------------------------------------------------------------------
-- dim_segment: Vehicle segments (PV / CV / 2W)
-- ---------------------------------------------------------------------------
CREATE TABLE dim_segment (
    segment_id    SERIAL      PRIMARY KEY,
    segment_code  VARCHAR(5)  NOT NULL,   -- 'PV', 'CV', '2W'
    segment_name  VARCHAR(50) NOT NULL,
    sub_segment   VARCHAR(50),            -- 'LCV', 'MHCV', 'Motorcycle', 'Scooter', etc.
    UNIQUE (segment_code, sub_segment)
);

COMMENT ON TABLE dim_segment IS 'Segment dimension. V1 uses top-level codes (PV/CV/2W). Sub-segments for future drill-down.';

-- ---------------------------------------------------------------------------
-- dim_vehicle_class_map: VAHAN vehicle class → segment mapping
-- ---------------------------------------------------------------------------
CREATE TABLE dim_vehicle_class_map (
    map_id           SERIAL       PRIMARY KEY,
    vahan_class_name VARCHAR(100) NOT NULL UNIQUE,
    segment_id       INT          REFERENCES dim_segment(segment_id),
    is_excluded      BOOLEAN      NOT NULL DEFAULT FALSE,
    notes            TEXT
);

CREATE INDEX idx_dim_vcm_lookup ON dim_vehicle_class_map (vahan_class_name);

COMMENT ON TABLE dim_vehicle_class_map IS 'Maps VAHAN vehicle class names to segments. is_excluded=TRUE for 3W, tractors, etc. Unmapped classes trigger alert.';

-- ---------------------------------------------------------------------------
-- dim_fuel: Fuel type → powertrain bucket mapping
-- ---------------------------------------------------------------------------
CREATE TABLE dim_fuel (
    fuel_id          SERIAL      PRIMARY KEY,
    fuel_code        VARCHAR(50) NOT NULL UNIQUE,
    powertrain       VARCHAR(10) NOT NULL CHECK (powertrain IN ('ICE', 'EV', 'HYBRID')),
    dashboard_bucket VARCHAR(5)  NOT NULL CHECK (dashboard_bucket IN ('ICE', 'EV')),
    fuel_group       VARCHAR(20) NOT NULL  -- 'Petrol', 'Diesel', 'CNG', 'CNG/LPG', 'Hybrid', 'Electric', 'Other'
);

COMMENT ON TABLE dim_fuel IS 'Fuel type dimension. dashboard_bucket is the 2-bucket view (ICE/EV). fuel_group for drill-down. Hybrids roll into ICE at top level.';

-- ---------------------------------------------------------------------------
-- dim_geo: Geography dimension (V1: National only; V2: State/RTO)
-- ---------------------------------------------------------------------------
CREATE TABLE dim_geo (
    geo_id        SERIAL      PRIMARY KEY,
    level         VARCHAR(10) NOT NULL CHECK (level IN ('NATIONAL', 'STATE', 'RTO')),
    state_name    VARCHAR(100),
    rto_code      VARCHAR(20),
    rto_name      VARCHAR(150),
    vahan4_active BOOLEAN     NOT NULL DEFAULT FALSE
);

CREATE INDEX idx_dim_geo_state ON dim_geo (state_name) WHERE state_name IS NOT NULL;

COMMENT ON TABLE dim_geo IS 'Geography dimension. V1 uses only geo_id=1 (All India). Schema supports V2 state/RTO drill-down.';


-- ============================================================================
-- BRONZE TABLES (Raw / Immutable)
-- ============================================================================

-- ---------------------------------------------------------------------------
-- raw_extraction_log: Tracks every ETL run
-- ---------------------------------------------------------------------------
CREATE TABLE raw_extraction_log (
    run_id           SERIAL       PRIMARY KEY,
    source           VARCHAR(20)  NOT NULL CHECK (source IN ('VAHAN', 'FADA', 'BSE', 'SIAM', 'VONTER', 'MANUAL')),
    started_at       TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    completed_at     TIMESTAMPTZ,
    status           VARCHAR(20)  NOT NULL DEFAULT 'RUNNING' CHECK (status IN ('RUNNING', 'SUCCESS', 'PARTIAL', 'FAILED', 'VALIDATION_FAILED')),
    records_extracted INT,
    records_loaded   INT,
    error_message    TEXT,
    notes            TEXT
);

CREATE INDEX idx_raw_log_source_status ON raw_extraction_log (source, status);
CREATE INDEX idx_raw_log_started ON raw_extraction_log (started_at DESC);

COMMENT ON TABLE raw_extraction_log IS 'Audit log for every ETL extraction run. Immutable. Used for lineage and debugging.';

-- ---------------------------------------------------------------------------
-- raw_vahan_snapshot: Raw VAHAN extraction data (immutable)
-- ---------------------------------------------------------------------------
CREATE TABLE raw_vahan_snapshot (
    id                 BIGSERIAL    PRIMARY KEY,
    run_id             INT          NOT NULL REFERENCES raw_extraction_log(run_id),
    data_period        VARCHAR(20)  NOT NULL,   -- e.g. '2026-03' or '2026-03-04'
    state_filter       VARCHAR(100) NOT NULL DEFAULT 'ALL',
    vehicle_category   VARCHAR(50),             -- Y-axis category
    vehicle_class      VARCHAR(100),
    fuel               VARCHAR(50),
    maker              VARCHAR(150),
    registration_count BIGINT       NOT NULL CHECK (registration_count >= 0),
    query_params       JSONB,                   -- Captures exact request params for reproducibility
    created_at         TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_raw_vahan_run ON raw_vahan_snapshot (run_id);
CREATE INDEX idx_raw_vahan_period ON raw_vahan_snapshot (data_period);
CREATE INDEX idx_raw_vahan_maker ON raw_vahan_snapshot (maker);

COMMENT ON TABLE raw_vahan_snapshot IS 'Raw, immutable VAHAN extraction data. One row per maker × fuel × vehicle_class per extraction run. Aggregated counts only — no PII.';

-- ---------------------------------------------------------------------------
-- raw_fada_monthly: Raw FADA PDF extraction data
-- ---------------------------------------------------------------------------
CREATE TABLE raw_fada_monthly (
    id                  SERIAL       PRIMARY KEY,
    run_id              INT          NOT NULL REFERENCES raw_extraction_log(run_id),
    report_month        DATE         NOT NULL,  -- First day of the reported month
    category            VARCHAR(50)  NOT NULL,   -- e.g. 'Passenger Vehicles', 'Commercial Vehicles', '2-Wheeler'
    oem_name            VARCHAR(150) NOT NULL,
    volume_current      BIGINT,
    volume_prior_year   BIGINT,
    yoy_pct             DECIMAL(8,2),
    market_share_pct    DECIMAL(6,2),
    fuel_type           VARCHAR(50),
    source_file         VARCHAR(200),
    created_at          TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_raw_fada_month ON raw_fada_monthly (report_month);

COMMENT ON TABLE raw_fada_monthly IS 'Raw data extracted from FADA monthly press release PDFs. Used for QA/reconciliation against VAHAN.';

-- ---------------------------------------------------------------------------
-- raw_oem_wholesale: Raw monthly wholesale data from BSE filings
-- ---------------------------------------------------------------------------
CREATE TABLE raw_oem_wholesale (
    id               SERIAL       PRIMARY KEY,
    run_id           INT          NOT NULL REFERENCES raw_extraction_log(run_id),
    report_month     DATE         NOT NULL,
    oem_name         VARCHAR(150) NOT NULL,
    segment_code     VARCHAR(10),
    wholesale_units  BIGINT,
    source_url       VARCHAR(300),
    created_at       TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_raw_wholesale_month ON raw_oem_wholesale (report_month);

COMMENT ON TABLE raw_oem_wholesale IS 'Raw monthly wholesale dispatch data from BSE regulatory filings.';


-- ============================================================================
-- SILVER TABLES (Validated & Normalized)
-- ============================================================================

-- ---------------------------------------------------------------------------
-- fact_daily_registrations: Normalized daily retail registration data
-- ---------------------------------------------------------------------------
CREATE TABLE fact_daily_registrations (
    id               BIGSERIAL    PRIMARY KEY,
    date_key         DATE         NOT NULL REFERENCES dim_date(date_key),
    oem_id           INT          NOT NULL REFERENCES dim_oem(oem_id),
    segment_id       INT          NOT NULL REFERENCES dim_segment(segment_id),
    fuel_id          INT          NOT NULL REFERENCES dim_fuel(fuel_id),
    geo_id           INT          NOT NULL REFERENCES dim_geo(geo_id),
    run_id           INT          NOT NULL REFERENCES raw_extraction_log(run_id),
    registration_count BIGINT     NOT NULL CHECK (registration_count >= 0),
    is_revision      BOOLEAN      NOT NULL DEFAULT FALSE,
    created_at       TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    UNIQUE (date_key, oem_id, segment_id, fuel_id, geo_id)
);

CREATE INDEX idx_fact_daily_date ON fact_daily_registrations (date_key);
CREATE INDEX idx_fact_daily_oem ON fact_daily_registrations (oem_id);
CREATE INDEX idx_fact_daily_segment ON fact_daily_registrations (segment_id);

COMMENT ON TABLE fact_daily_registrations IS 'Normalized daily retail registration facts. Grain: date × OEM × segment × fuel × geo. Source: VAHAN via transform pipeline.';

-- ---------------------------------------------------------------------------
-- fact_monthly_registrations: Pre-aggregated monthly registration data
-- ---------------------------------------------------------------------------
CREATE TABLE fact_monthly_registrations (
    id               BIGSERIAL    PRIMARY KEY,
    month_key        DATE         NOT NULL,   -- First day of month
    oem_id           INT          NOT NULL REFERENCES dim_oem(oem_id),
    segment_id       INT          NOT NULL REFERENCES dim_segment(segment_id),
    fuel_id          INT          NOT NULL REFERENCES dim_fuel(fuel_id),
    geo_id           INT          NOT NULL REFERENCES dim_geo(geo_id),
    units            BIGINT       NOT NULL CHECK (units >= 0),
    mtd_as_of        DATE,
    is_full_month    BOOLEAN      NOT NULL DEFAULT FALSE,
    created_at       TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at       TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    UNIQUE (month_key, oem_id, segment_id, fuel_id, geo_id)
);

CREATE INDEX idx_fact_monthly_month ON fact_monthly_registrations (month_key);
CREATE INDEX idx_fact_monthly_oem ON fact_monthly_registrations (oem_id);

COMMENT ON TABLE fact_monthly_registrations IS 'Pre-aggregated monthly registration facts. Grain: month × OEM × segment × fuel × geo. Updated daily during month, flagged is_full_month=TRUE after EOM.';

-- ---------------------------------------------------------------------------
-- fact_monthly_wholesale: Monthly wholesale dispatch facts
-- ---------------------------------------------------------------------------
CREATE TABLE fact_monthly_wholesale (
    id               SERIAL       PRIMARY KEY,
    month_key        DATE         NOT NULL,
    oem_id           INT          NOT NULL REFERENCES dim_oem(oem_id),
    segment_id       INT          NOT NULL REFERENCES dim_segment(segment_id),
    units            BIGINT       NOT NULL CHECK (units >= 0),
    source           VARCHAR(20)  NOT NULL DEFAULT 'BSE',
    created_at       TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    UNIQUE (month_key, oem_id, segment_id)
);

CREATE INDEX idx_fact_wholesale_month ON fact_monthly_wholesale (month_key);

COMMENT ON TABLE fact_monthly_wholesale IS 'Monthly wholesale dispatch facts. Source: BSE regulatory filings.';


-- ============================================================================
-- GOLD TABLES (Business-Ready)
-- ============================================================================

-- ---------------------------------------------------------------------------
-- fact_asp_master: ASP assumptions for revenue proxy calculation
-- ---------------------------------------------------------------------------
CREATE TABLE fact_asp_master (
    asp_id           SERIAL       PRIMARY KEY,
    oem_id           INT          NOT NULL REFERENCES dim_oem(oem_id),
    segment_id       INT          NOT NULL REFERENCES dim_segment(segment_id),
    fuel_id          INT          NOT NULL,  -- Not FK; can be 0 for 'all fuels'
    effective_from   DATE         NOT NULL,
    effective_to     DATE,
    asp_inr_lakhs    DECIMAL(10,4) NOT NULL CHECK (asp_inr_lakhs > 0),
    source           VARCHAR(50)  NOT NULL DEFAULT 'ANALYST_ESTIMATE',
    notes            TEXT,
    created_at       TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    UNIQUE (oem_id, segment_id, fuel_id, effective_from)
);

COMMENT ON TABLE fact_asp_master IS 'ASP (Average Selling Price) assumptions used to estimate revenue proxies. Source = ANALYST_ESTIMATE until calibrated from earnings.';

-- ---------------------------------------------------------------------------
-- est_quarterly_revenue: Estimated quarterly revenue proxy
-- ---------------------------------------------------------------------------
CREATE TABLE est_quarterly_revenue (
    id               SERIAL       PRIMARY KEY,
    fy_quarter       VARCHAR(8)   NOT NULL,   -- e.g. 'Q3FY26'
    oem_id           INT          NOT NULL REFERENCES dim_oem(oem_id),
    segment_id       INT          NOT NULL REFERENCES dim_segment(segment_id),
    units_retail     BIGINT,
    units_wholesale  BIGINT,
    asp_used         DECIMAL(10,4),
    revenue_retail_cr DECIMAL(14,2),  -- INR Crore
    revenue_wholesale_cr DECIMAL(14,2),
    data_completeness DECIMAL(5,2),   -- % of trading days covered
    generated_at     TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    UNIQUE (fy_quarter, oem_id, segment_id)
);

CREATE INDEX idx_est_rev_quarter ON est_quarterly_revenue (fy_quarter);

COMMENT ON TABLE est_quarterly_revenue IS 'Estimated quarterly revenue proxies. Grain: FY quarter × OEM × segment. Revenue = units × ASP assumption. Retail = VAHAN registrations. Wholesale = BSE filings.';


-- ============================================================================
-- MATERIALIZED VIEW
-- ============================================================================

-- ---------------------------------------------------------------------------
-- mv_oem_monthly_summary: Pre-aggregated OEM monthly summary for dashboard
-- ---------------------------------------------------------------------------
CREATE MATERIALIZED VIEW mv_oem_monthly_summary AS
SELECT
    fmr.month_key,
    dd.fy_year,
    dd.fy_quarter,
    do2.oem_id,
    do2.oem_name,
    do2.nse_ticker,
    ds.segment_code,
    df.dashboard_bucket  AS fuel_bucket,
    SUM(fmr.units)       AS total_units,
    SUM(fmr.units) FILTER (WHERE dd.calendar_year = EXTRACT(YEAR FROM fmr.month_key - INTERVAL '1 year')
        AND dd.calendar_month = EXTRACT(MONTH FROM fmr.month_key)) AS units_prior_year,
    MAX(fmr.updated_at)  AS last_updated
FROM fact_monthly_registrations fmr
JOIN dim_date   dd  ON dd.date_key   = fmr.month_key
JOIN dim_oem    do2 ON do2.oem_id    = fmr.oem_id
JOIN dim_segment ds ON ds.segment_id = fmr.segment_id
JOIN dim_fuel   df  ON df.fuel_id    = fmr.fuel_id
WHERE do2.is_in_scope = TRUE
GROUP BY
    fmr.month_key, dd.fy_year, dd.fy_quarter,
    do2.oem_id, do2.oem_name, do2.nse_ticker,
    ds.segment_code, df.dashboard_bucket;

CREATE UNIQUE INDEX idx_mv_oem_monthly_pk
    ON mv_oem_monthly_summary (month_key, oem_id, segment_code, fuel_bucket);
CREATE INDEX idx_mv_oem_monthly_ticker
    ON mv_oem_monthly_summary (nse_ticker, month_key);
CREATE INDEX idx_mv_oem_monthly_quarter
    ON mv_oem_monthly_summary (fy_quarter, oem_id);

COMMENT ON MATERIALIZED VIEW mv_oem_monthly_summary IS
    'Pre-aggregated OEM monthly summary for dashboard. Refreshed after each successful ETL run.';
