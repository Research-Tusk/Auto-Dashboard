/**
 * AutoQuant — TypeScript Interfaces
 * Shared type definitions for all dashboard components.
 */

// ---------------------------------------------------------------------------
// Database row types (mirror DB column names)
// ---------------------------------------------------------------------------

export interface TIVRow {
  month_key: string;          // 'YYYY-MM-DD' (first of month)
  fy_quarter: string;         // e.g. 'Q3FY26'
  segment_code: string;       // 'PV' | 'CV' | '2W'
  tiv_units: number;
  ev_units: number;
  ev_penetration_pct: number | null;
}

export interface OEMShareRow {
  month_key: string;
  fy_quarter: string;
  segment_code: string;
  oem_name: string;
  nse_ticker: string | null;
  units: number;
  market_share_pct: number;
}

// ---------------------------------------------------------------------------
// Dimension / metadata
// ---------------------------------------------------------------------------

export interface OEMRecord {
  oem_id: number;
  oem_name: string;
  nse_ticker: string | null;
  primary_segments: string[];
  is_in_scope: boolean;
  description: string | null;
}

// ---------------------------------------------------------------------------
// Fact tables (Supabase views)
// ---------------------------------------------------------------------------

export interface TIVRecord {
  fy_quarter: string;
  segment_id: number;
  tiv_retail: number | null;
  tiv_wholesale: number | null;
  tiv_yoy_pct: number | null;
}

export interface OEMScorecard {
  fy_quarter: string;
  oem_id: number;
  segment_id: number;
  units_retail_qtd: number | null;
  units_wholesale_qtd: number | null;
  revenue_retail_cr: number | null;
  ms_pct: number | null;
  ms_pct_qoq: number | null;
  ms_pct_yoy: number | null;
  rank_by_ms: number | null;
}

export interface MSHistoryRecord {
  fy_quarter: string;
  oem_id: number;
  segment_id: number;
  units_retail: number | null;
  ms_pct: number | null;
  ms_pct_qoq: number | null;
  ms_pct_yoy: number | null;
}

export interface RevenueRow {
  fy_quarter: string;
  oem_id: number;
  segment_id: number;
  units_retail: number | null;
  units_wholesale: number | null;
  asp_used: number | null;
  revenue_retail_cr: number | null;
  revenue_wholesale_cr: number | null;
  data_completeness: number | null;
  generated_at: string | null;
}

export interface PowertrainRow {
  fy_quarter: string;
  oem_id: number;
  segment_id: number;
  powertrain: string;
  units_retail: number | null;
  share_pct: number | null;
}

// ---------------------------------------------------------------------------
// Chart-specific types
// ---------------------------------------------------------------------------

export type SegmentCode = 'PV' | 'CV' | '2W';

export interface TIVChartPoint {
  quarter: string;
  tiv: number;
  yoy_pct: number | null;
}

export interface MSChartPoint {
  quarter: string;
  [oemName: string]: number | string;
}

export interface RevenueChartPoint {
  quarter: string;         // used as X-axis label (may be OEM name for bar chart)
  revenue_cr: number;
  units: number;
  asp: number;
}

export interface PowertrainChartPoint {
  quarter: string;
  [powertrain: string]: number | string;
}
