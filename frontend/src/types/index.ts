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
  oem_name: string;
  nse_ticker: string | null;
  segment_code: string;
  oem_units: number;
  segment_tiv: number;
  market_share_pct: number | null;
}

export interface OEMSummaryRow {
  month_key: string;
  fy_year: string;
  fy_quarter: string;
  oem_name: string;
  segment_code: string;
  fuel_bucket: 'ICE' | 'EV';
  total_units: number;
  units_prior_year: number | null;
  last_updated: string | null;
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
  generated_at: string;
}

export interface OEMInfo {
  oem_id: number;
  oem_name: string;
  nse_ticker: string | null;
  is_listed: boolean;
  is_in_scope: boolean;
  primary_segments: string[];
}

export interface FreshnesRow {
  source: string;
  last_attempted: string | null;
  last_success: string | null;
  last_records_loaded: number | null;
  failures_24h: number;
}

// ---------------------------------------------------------------------------
// Chart data types
// ---------------------------------------------------------------------------

export interface TIVChartPoint {
  month: string;              // formatted for display: 'Dec 25'
  total: number;
  ev: number;
  evPct: number;
}

export interface OEMShareChartPoint {
  month: string;
  [oemName: string]: string | number;  // dynamic OEM keys
}

export interface RevenueChartPoint {
  quarter: string;
  revenue_cr: number;
  units: number;
  asp: number;
}

// ---------------------------------------------------------------------------
// API response types
// ---------------------------------------------------------------------------

export interface DashboardAPIResponse {
  tiv: TIVRow[];
  share: OEMShareRow[];
}

export interface OEMAPIResponse {
  monthly: OEMSummaryRow[];
  revenue: RevenueRow[];
  share: OEMShareRow[];
}

export interface RevenueAPIResponse {
  data: RevenueRow[];
}

// ---------------------------------------------------------------------------
// UI state types
// ---------------------------------------------------------------------------

export type SegmentCode = 'PV' | 'CV' | '2W';
export type FuelBucket = 'ICE' | 'EV';
export type FYQuarter = string; // e.g. 'Q3FY26'

export interface FilterState {
  segment: SegmentCode;
  fromYear: number;
  toYear: number;
  oemTicker?: string;
}
