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
    segment          VARCHAR(50),
    domestic_volume  BIGINT,
    export_volume    BIGINT,
    total_volume     BIGINT,
    source_url       VARCHAR(500),
    created_at       TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_raw_wholesale_month ON raw_oem_wholesale (report_month);

COMMENT ON TABLE raw_oem_wholesale IS 'Raw monthly wholesale (factory dispatch) data from BSE filings. NOT registrations — this is dispatch data.';


-- ============================================================================
-- SILVER TABLES (Normalized / Transformed)
-- ============================================================================

-- ---------------------------------------------------------------------------
-- fact_daily_registrations: Core fact table — daily registrations
-- ---------------------------------------------------------------------------
CREATE TABLE fact_daily_registrations (
    id              BIGSERIAL    PRIMARY KEY,
    data_date       DATE         NOT NULL,
    geo_id          INT          NOT NULL DEFAULT 1 REFERENCES dim_geo(geo_id),
    oem_id          INT          NOT NULL REFERENCES dim_oem(oem_id),
    segment_id      INT          NOT NULL REFERENCES dim_segment(segment_id),
    fuel_id         INT          NOT NULL REFERENCES dim_fuel(fuel_id),
    registrations   BIGINT       NOT NULL CHECK (registrations >= 0),
    source          VARCHAR(20)  NOT NULL DEFAULT 'VAHAN',
    run_id          INT          REFERENCES raw_extraction_log(run_id),
    revision_num    INT          NOT NULL DEFAULT 1,
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

-- Performance indexes for dashboard queries
CREATE INDEX idx_fdr_date ON fact_daily_registrations (data_date);
CREATE INDEX idx_fdr_oem_date ON fact_daily_registrations (oem_id, data_date);
CREATE INDEX idx_fdr_segment_date ON fact_daily_registrations (segment_id, data_date);
CREATE INDEX idx_fdr_fuel_date ON fact_daily_registrations (fuel_id, data_date);
CREATE INDEX idx_fdr_composite ON fact_daily_registrations (data_date, oem_id, segment_id, fuel_id);

-- Unique constraint on the grain (latest revision wins)
-- We use a unique index on the grain excluding revision_num to enable upserts
CREATE UNIQUE INDEX idx_fdr_grain ON fact_daily_registrations (data_date, geo_id, oem_id, segment_id, fuel_id, source, revision_num);

COMMENT ON TABLE fact_daily_registrations IS 'Daily vehicle registration volumes. Grain: date × geo × OEM × segment × fuel. Supports revision tracking via revision_num.';

-- ---------------------------------------------------------------------------
-- fact_monthly_registrations: Aggregated monthly registrations
-- ---------------------------------------------------------------------------
CREATE TABLE fact_monthly_registrations (
    id              SERIAL       PRIMARY KEY,
    month_date      DATE         NOT NULL,  -- First day of month
    oem_id          INT          NOT NULL REFERENCES dim_oem(oem_id),
    segment_id      INT          NOT NULL REFERENCES dim_segment(segment_id),
    fuel_id         INT          NOT NULL REFERENCES dim_fuel(fuel_id),
    registrations   BIGINT       NOT NULL CHECK (registrations >= 0),
    source          VARCHAR(20)  NOT NULL,  -- 'VAHAN', 'FADA', 'SIAM', etc.
    is_final        BOOLEAN      NOT NULL DEFAULT FALSE,
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    UNIQUE (month_date, oem_id, segment_id, fuel_id, source)
);

CREATE INDEX idx_fmr_month ON fact_monthly_registrations (month_date);
CREATE INDEX idx_fmr_oem_month ON fact_monthly_registrations (oem_id, month_date);

COMMENT ON TABLE fact_monthly_registrations IS 'Monthly aggregated registrations. is_final=TRUE after monthly reconciliation with FADA. Multiple sources may coexist.';

-- ---------------------------------------------------------------------------
-- fact_monthly_wholesale: Monthly wholesale (factory dispatch) volumes
-- ---------------------------------------------------------------------------
CREATE TABLE fact_monthly_wholesale (
    id              SERIAL       PRIMARY KEY,
    month_date      DATE         NOT NULL,
    oem_id          INT          NOT NULL REFERENCES dim_oem(oem_id),
    segment_id      INT          NOT NULL REFERENCES dim_segment(segment_id),
    domestic_volume BIGINT,
    export_volume   BIGINT,
    total_volume    BIGINT,
    source          VARCHAR(20)  NOT NULL,
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    UNIQUE (month_date, oem_id, segment_id, source)
);

CREATE INDEX idx_fmw_oem_month ON fact_monthly_wholesale (oem_id, month_date);

COMMENT ON TABLE fact_monthly_wholesale IS 'Monthly wholesale (factory dispatch) volumes from BSE filings. Used for channel inventory signal (wholesale - registrations).';


-- ============================================================================
-- GOLD TABLES (Analytics & Financial Proxy)
-- ============================================================================

-- ---------------------------------------------------------------------------
-- fact_asp_master: Average Selling Price assumptions by segment × powertrain
-- ---------------------------------------------------------------------------
CREATE TABLE fact_asp_master (
    id                    SERIAL        PRIMARY KEY,
    segment_id            INT           NOT NULL REFERENCES dim_segment(segment_id),
    fuel_id               INT           REFERENCES dim_fuel(fuel_id),  -- NULL = segment-level ASP
    effective_from        DATE          NOT NULL,
    effective_to          DATE,         -- NULL = currently active
    asp_ex_factory_rupees DECIMAL(14,2) NOT NULL CHECK (asp_ex_factory_rupees > 0),
    asp_source            VARCHAR(20)   NOT NULL CHECK (asp_source IN ('BACKCALC', 'ESTIMATED', 'ANNOUNCED', 'CALIBRATED')),
    confidence            VARCHAR(10)   NOT NULL CHECK (confidence IN ('HIGH', 'MEDIUM', 'LOW')),
    notes                 TEXT
);

CREATE INDEX idx_asp_effective ON fact_asp_master (segment_id, effective_from, effective_to);

COMMENT ON TABLE fact_asp_master IS 'ASP assumptions for revenue proxy. Calibrated quarterly from OEM earnings. PROMINENT DISCLAIMER: "Demand-based proxy using registrations × assumed ASPs. This is NOT accounting revenue."';

-- ---------------------------------------------------------------------------
-- est_quarterly_revenue: Estimated quarterly revenue proxy
-- ---------------------------------------------------------------------------
CREATE TABLE est_quarterly_revenue (
    id                       SERIAL         PRIMARY KEY,
    oem_id                   INT            NOT NULL REFERENCES dim_oem(oem_id),
    fy_quarter               VARCHAR(8)     NOT NULL,   -- e.g. 'Q3FY26'
    estimate_date            DATE           NOT NULL,    -- When this estimate was computed
    reg_volume               BIGINT,
    wholesale_volume         BIGINT,
    export_volume            BIGINT,
    est_domestic_rev_cr      DECIMAL(15,2), -- Estimated domestic revenue in ₹ Crores
    est_total_rev_cr         DECIMAL(15,2), -- Including export estimate
    est_rev_low_cr           DECIMAL(15,2), -- Low bound (ASP -5%, Vol -5%)
    est_rev_high_cr          DECIMAL(15,2), -- High bound (ASP +5%, Vol +5%)
    data_completeness_pct    DECIMAL(5,2),  -- % of quarter days with data
    created_at               TIMESTAMPTZ    NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_eqr_oem_quarter ON est_quarterly_revenue (oem_id, fy_quarter);

COMMENT ON TABLE est_quarterly_revenue IS 'Demand-based implied revenue proxy. PROMINENT DISCLAIMER: This is NOT accounting revenue. Uses registrations × assumed ASPs.';


-- ============================================================================
-- MATERIALIZED VIEW
-- ============================================================================

-- ---------------------------------------------------------------------------
-- mv_oem_monthly_summary: Pre-aggregated OEM monthly registrations
-- ---------------------------------------------------------------------------
CREATE MATERIALIZED VIEW mv_oem_monthly_summary AS
SELECT
    dd.calendar_year,
    dd.calendar_month,
    dd.fy_year,
    dd.fy_quarter,
    o.oem_name,
    o.nse_ticker,
    o.is_listed,
    s.segment_code,
    f.dashboard_bucket AS powertrain,
    SUM(fdr.registrations) AS total_registrations
FROM fact_daily_registrations fdr
JOIN dim_date dd       ON fdr.data_date = dd.date_key
JOIN dim_oem o         ON fdr.oem_id = o.oem_id
JOIN dim_segment s     ON fdr.segment_id = s.segment_id
JOIN dim_fuel f        ON fdr.fuel_id = f.fuel_id
WHERE fdr.registrations > 0
GROUP BY
    dd.calendar_year,
    dd.calendar_month,
    dd.fy_year,
    dd.fy_quarter,
    o.oem_name,
    o.nse_ticker,
    o.is_listed,
    s.segment_code,
    f.dashboard_bucket;

CREATE UNIQUE INDEX idx_mv_oms_unique ON mv_oem_monthly_summary (
    calendar_year, calendar_month, oem_name, segment_code, powertrain
);
CREATE INDEX idx_mv_oms_ticker ON mv_oem_monthly_summary (nse_ticker);
CREATE INDEX idx_mv_oms_fy ON mv_oem_monthly_summary (fy_year, fy_quarter);

COMMENT ON MATERIALIZED VIEW mv_oem_monthly_summary IS 'Pre-aggregated monthly OEM registrations for dashboard queries. Refreshed after each daily/weekly ETL run via: REFRESH MATERIALIZED VIEW CONCURRENTLY mv_oem_monthly_summary;';


-- ============================================================================
-- HELPER FUNCTIONS
-- ============================================================================

-- Function to derive Indian Financial Year from a date
CREATE OR REPLACE FUNCTION get_fy_year(d DATE) RETURNS VARCHAR(6) AS $$
DECLARE
    suffix INT;
BEGIN
    IF EXTRACT(MONTH FROM d) >= 4 THEN
        suffix := (EXTRACT(YEAR FROM d)::INT + 1) % 100;
    ELSE
        suffix := EXTRACT(YEAR FROM d)::INT % 100;
    END IF;
    RETURN 'FY' || LPAD(suffix::TEXT, 2, '0');
END;
$$ LANGUAGE plpgsql IMMUTABLE;

-- Function to derive FY quarter from a date
CREATE OR REPLACE FUNCTION get_fy_quarter(d DATE) RETURNS VARCHAR(8) AS $$
DECLARE
    m INT := EXTRACT(MONTH FROM d)::INT;
    y INT := EXTRACT(YEAR FROM d)::INT;
    fy_suffix TEXT;
    q_num INT;
BEGIN
    IF m >= 4 THEN
        fy_suffix := ((y + 1) % 100)::TEXT;
    ELSE
        fy_suffix := (y % 100)::TEXT;
    END IF;

    IF LENGTH(fy_suffix) = 1 THEN
        fy_suffix := '0' || fy_suffix;
    END IF;

    CASE
        WHEN m IN (4, 5, 6)   THEN q_num := 1;
        WHEN m IN (7, 8, 9)   THEN q_num := 2;
        WHEN m IN (10, 11, 12) THEN q_num := 3;
        ELSE q_num := 4;  -- Jan, Feb, Mar
    END CASE;

    RETURN 'Q' || q_num || 'FY' || fy_suffix;
END;
$$ LANGUAGE plpgsql IMMUTABLE;

-- Function to get FY quarter number (1-4) from a date
CREATE OR REPLACE FUNCTION get_fy_quarter_num(d DATE) RETURNS SMALLINT AS $$
DECLARE
    m INT := EXTRACT(MONTH FROM d)::INT;
BEGIN
    CASE
        WHEN m IN (4, 5, 6)   THEN RETURN 1;
        WHEN m IN (7, 8, 9)   THEN RETURN 2;
        WHEN m IN (10, 11, 12) THEN RETURN 3;
        ELSE RETURN 4;
    END CASE;
END;
$$ LANGUAGE plpgsql IMMUTABLE;
