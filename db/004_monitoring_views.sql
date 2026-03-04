-- ============================================================================
-- AutoQuant: Monitoring Views & Heartbeat Table
-- Provides pipeline health visibility for the monitor.py module
-- ============================================================================

-- ---------------------------------------------------------------------------
-- pipeline_heartbeat: Lightweight table for aliveness checks
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS pipeline_heartbeat (
    id          SERIAL      PRIMARY KEY,
    checked_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    status      VARCHAR(10) NOT NULL DEFAULT 'OK',
    note        TEXT
);

COMMENT ON TABLE pipeline_heartbeat IS 'Lightweight heartbeat table. Written to every health check run. Used to verify DB connectivity and write permissions.';

-- ---------------------------------------------------------------------------
-- v_pipeline_status: Overall pipeline status summary
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW v_pipeline_status AS
SELECT
    'last_successful_run'          AS metric,
    MAX(completed_at)::TEXT        AS value
FROM raw_extraction_log
WHERE status = 'SUCCESS'

UNION ALL

SELECT
    'last_run_status',
    status
FROM raw_extraction_log
ORDER BY started_at DESC
LIMIT 1

UNION ALL

SELECT
    'total_runs_today',
    COUNT(*)::TEXT
FROM raw_extraction_log
WHERE started_at >= CURRENT_DATE

UNION ALL

SELECT
    'failed_runs_today',
    COUNT(*)::TEXT
FROM raw_extraction_log
WHERE started_at >= CURRENT_DATE
  AND status IN ('FAILED', 'VALIDATION_FAILED')

UNION ALL

SELECT
    'total_daily_records',
    COUNT(*)::TEXT
FROM fact_daily_registrations
WHERE date_key = CURRENT_DATE - 1

UNION ALL

SELECT
    'days_since_last_full_month',
    COALESCE(
        (CURRENT_DATE - MAX(month_key))::TEXT,
        'N/A'
    )
FROM fact_monthly_registrations
WHERE is_full_month = TRUE;

COMMENT ON VIEW v_pipeline_status IS 'Quick pipeline health summary. Used by monitor.py.';

-- ---------------------------------------------------------------------------
-- v_extraction_log_recent: Recent extraction runs (last 7 days)
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW v_extraction_log_recent AS
SELECT
    run_id,
    source,
    started_at,
    completed_at,
    status,
    records_extracted,
    records_loaded,
    ROUND(EXTRACT(EPOCH FROM (completed_at - started_at)) / 60, 1) AS duration_minutes,
    error_message,
    notes
FROM raw_extraction_log
WHERE started_at >= NOW() - INTERVAL '7 days'
ORDER BY started_at DESC;

COMMENT ON VIEW v_extraction_log_recent IS 'Recent extraction runs for health monitoring.';

-- ---------------------------------------------------------------------------
-- v_data_freshness: Data freshness by source
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW v_data_freshness AS
SELECT
    source,
    MAX(started_at)                          AS last_attempted,
    MAX(CASE WHEN status = 'SUCCESS' THEN completed_at END) AS last_success,
    MAX(CASE WHEN status = 'SUCCESS' THEN records_loaded END) AS last_records_loaded,
    COUNT(*) FILTER (WHERE status = 'FAILED' AND started_at >= NOW() - INTERVAL '24 hours')
        AS failures_24h
FROM raw_extraction_log
GROUP BY source
ORDER BY source;

COMMENT ON VIEW v_data_freshness IS 'Data freshness summary by source. Used in health check dashboard.';

-- ---------------------------------------------------------------------------
-- v_unmapped_makers: Makers that appeared in raw data but have no alias
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW v_unmapped_makers AS
SELECT DISTINCT
    rvs.maker          AS raw_maker_name,
    'VAHAN'            AS source,
    COUNT(*)           AS occurrence_count,
    MAX(rvs.created_at) AS last_seen
FROM raw_vahan_snapshot rvs
WHERE NOT EXISTS (
    SELECT 1
    FROM dim_oem_alias a
    WHERE a.alias_name = rvs.maker
      AND a.source = 'VAHAN'
      AND a.is_active = TRUE
)
  AND rvs.maker IS NOT NULL
  AND rvs.maker != ''
GROUP BY rvs.maker
ORDER BY occurrence_count DESC;

COMMENT ON VIEW v_unmapped_makers IS 'Makers in raw data that have no alias in dim_oem_alias. Used to detect new entrants or name changes.';

-- ---------------------------------------------------------------------------
-- v_monthly_tiv_summary: Total Industry Volume (TIV) by month and segment
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW v_monthly_tiv_summary AS
SELECT
    fmr.month_key,
    dd.fy_quarter,
    ds.segment_code,
    SUM(fmr.units)                                      AS tiv_units,
    SUM(fmr.units) FILTER (WHERE df.dashboard_bucket = 'EV') AS ev_units,
    ROUND(
        100.0 * SUM(fmr.units) FILTER (WHERE df.dashboard_bucket = 'EV')
        / NULLIF(SUM(fmr.units), 0),
        2
    )                                                   AS ev_penetration_pct
FROM fact_monthly_registrations fmr
JOIN dim_date    dd ON dd.date_key   = fmr.month_key
JOIN dim_segment ds ON ds.segment_id = fmr.segment_id AND ds.sub_segment IS NULL
JOIN dim_fuel    df ON df.fuel_id    = fmr.fuel_id
GROUP BY fmr.month_key, dd.fy_quarter, ds.segment_code
ORDER BY fmr.month_key DESC, ds.segment_code;

COMMENT ON VIEW v_monthly_tiv_summary IS 'Monthly Total Industry Volume by segment with EV penetration. Used in Industry Pulse dashboard.';

-- ---------------------------------------------------------------------------
-- v_oem_market_share: OEM market share by month
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW v_oem_market_share AS
WITH monthly_tiv AS (
    SELECT
        fmr.month_key,
        fmr.segment_id,
        SUM(fmr.units) AS segment_tiv
    FROM fact_monthly_registrations fmr
    GROUP BY fmr.month_key, fmr.segment_id
)
SELECT
    fmr.month_key,
    do2.oem_name,
    do2.nse_ticker,
    ds.segment_code,
    SUM(fmr.units) AS oem_units,
    mt.segment_tiv,
    ROUND(
        100.0 * SUM(fmr.units) / NULLIF(mt.segment_tiv, 0),
        2
    )              AS market_share_pct
FROM fact_monthly_registrations fmr
JOIN dim_oem     do2 ON do2.oem_id    = fmr.oem_id
JOIN dim_segment ds  ON ds.segment_id = fmr.segment_id AND ds.sub_segment IS NULL
JOIN monthly_tiv mt  ON mt.month_key  = fmr.month_key AND mt.segment_id = fmr.segment_id
WHERE do2.is_in_scope = TRUE
GROUP BY fmr.month_key, do2.oem_name, do2.nse_ticker, ds.segment_code, mt.segment_tiv
ORDER BY fmr.month_key DESC, ds.segment_code, oem_units DESC;

COMMENT ON VIEW v_oem_market_share IS 'OEM market share by month and segment. Used in OEM Deep Dive panel.';

-- ---------------------------------------------------------------------------
-- v_fada_reconciliation: VAHAN vs FADA monthly volume comparison
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW v_fada_reconciliation AS
SELECT
    DATE_TRUNC('month', rfm.report_month) AS report_month,
    rfm.category,
    rfm.oem_name                          AS fada_oem_name,
    rfm.volume_current                    AS fada_volume,
    SUM(fmr.units)                        AS vahan_volume,
    rfm.volume_current - SUM(fmr.units)   AS delta,
    ROUND(
        100.0 * ABS(rfm.volume_current - SUM(fmr.units))
        / NULLIF(rfm.volume_current, 0),
        2
    )                                     AS delta_pct
FROM raw_fada_monthly rfm
LEFT JOIN dim_oem_alias oa
       ON oa.alias_name = rfm.oem_name AND oa.source = 'FADA'
LEFT JOIN fact_monthly_registrations fmr
       ON fmr.oem_id   = oa.oem_id
      AND fmr.month_key = rfm.report_month
GROUP BY rfm.report_month, rfm.category, rfm.oem_name, rfm.volume_current
ORDER BY rfm.report_month DESC, ABS(rfm.volume_current - SUM(fmr.units)) DESC;

COMMENT ON VIEW v_fada_reconciliation IS 'VAHAN vs FADA monthly reconciliation view. Used to flag data quality issues.';
