/**
 * AutoQuant — Supabase Query Functions
 * Typed query helpers for each dashboard section.
 *
 * DISCLAIMER: Revenue figures are demand-based proxies.
 * NOT accounting revenue. Do NOT use for investment decisions.
 */

import { createClient } from './supabase';

// ---------------------------------------------------------------------------
// Types (mirrored from src/types/index.ts for convenience)
// ---------------------------------------------------------------------------
export type { TIVRow, OEMShareRow, RevenueRow, OEMSummaryRow } from '../types';

import type { TIVRow, OEMShareRow, RevenueRow, OEMSummaryRow, OEMInfo, FreshnesRow } from '../types';

// ---------------------------------------------------------------------------
// Dashboard queries
// ---------------------------------------------------------------------------

/**
 * Get monthly TIV for all segments (last N months).
 */
export async function getTIVSummary(months = 13): Promise<TIVRow[]> {
  const supabase = createClient();
  const { data, error } = await supabase
    .from('v_monthly_tiv_summary')
    .select('month_key,fy_quarter,segment_code,tiv_units,ev_units,ev_penetration_pct')
    .order('month_key', { ascending: false })
    .limit(months * 4); // 4 segments

  if (error) throw new Error(error.message);
  return (data ?? []) as TIVRow[];
}

/**
 * Get OEM market share for a segment (last N months).
 */
export async function getOEMMarketShare(
  segment: string,
  months = 6
): Promise<OEMShareRow[]> {
  const supabase = createClient();
  const { data, error } = await supabase
    .from('v_oem_market_share')
    .select('month_key,oem_name,nse_ticker,segment_code,oem_units,segment_tiv,market_share_pct')
    .eq('segment_code', segment.toUpperCase())
    .order('month_key', { ascending: false })
    .limit(months * 20); // ~20 OEMs per segment

  if (error) throw new Error(error.message);
  return (data ?? []) as OEMShareRow[];
}

// ---------------------------------------------------------------------------
// OEM Deep Dive queries
// ---------------------------------------------------------------------------

/**
 * Get monthly OEM summary from materialized view.
 */
export async function getOEMMonthly(
  ticker: string,
  months = 13
): Promise<OEMSummaryRow[]> {
  const supabase = createClient();
  const { data, error } = await supabase
    .from('mv_oem_monthly_summary')
    .select(
      'month_key,fy_year,fy_quarter,oem_name,segment_code,fuel_bucket,total_units,units_prior_year,last_updated'
    )
    .eq('nse_ticker', ticker.toUpperCase())
    .order('month_key', { ascending: false })
    .limit(months * 12);

  if (error) throw new Error(error.message);
  return (data ?? []) as OEMSummaryRow[];
}

/**
 * Get OEM revenue proxy history.
 */
export async function getOEMRevenue(
  oemId?: number,
  quarters = 8
): Promise<RevenueRow[]> {
  const supabase = createClient();
  let query = supabase
    .from('est_quarterly_revenue')
    .select(
      'fy_quarter,oem_id,segment_id,units_retail,units_wholesale,asp_used,revenue_retail_cr,revenue_wholesale_cr,data_completeness,generated_at'
    )
    .order('fy_quarter', { ascending: false })
    .limit(quarters * 10);

  if (oemId) {
    query = query.eq('oem_id', oemId);
  }

  const { data, error } = await query;
  if (error) throw new Error(error.message);
  return (data ?? []) as RevenueRow[];
}

// ---------------------------------------------------------------------------
// History queries
// ---------------------------------------------------------------------------

/**
 * Get historical TIV for a segment and year range.
 */
export async function getHistoricalTIV(
  segment: string,
  fromYear: number,
  toYear: number
): Promise<TIVRow[]> {
  const supabase = createClient();
  const { data, error } = await supabase
    .from('v_monthly_tiv_summary')
    .select('month_key,fy_quarter,tiv_units,ev_units,ev_penetration_pct')
    .eq('segment_code', segment.toUpperCase())
    .gte('month_key', `${fromYear}-01-01`)
    .lte('month_key', `${toYear}-12-31`)
    .order('month_key', { ascending: true });

  if (error) throw new Error(error.message);
  return (data ?? []) as TIVRow[];
}

/**
 * Get historical OEM share for a segment and year range.
 */
export async function getHistoricalOEMShare(
  segment: string,
  fromYear: number,
  toYear: number,
  limit = 500
): Promise<OEMShareRow[]> {
  const supabase = createClient();
  const { data, error } = await supabase
    .from('v_oem_market_share')
    .select('month_key,oem_name,nse_ticker,oem_units,market_share_pct')
    .eq('segment_code', segment.toUpperCase())
    .gte('month_key', `${fromYear}-01-01`)
    .lte('month_key', `${toYear}-12-31`)
    .order('month_key', { ascending: true })
    .limit(limit);

  if (error) throw new Error(error.message);
  return (data ?? []) as OEMShareRow[];
}

// ---------------------------------------------------------------------------
// Admin / debug queries
// ---------------------------------------------------------------------------

/**
 * Get all in-scope OEMs.
 */
export async function getOEMs(): Promise<OEMInfo[]> {
  const supabase = createClient();
  const { data, error } = await supabase
    .from('dim_oem')
    .select('oem_id,oem_name,nse_ticker,is_listed,is_in_scope,primary_segments')
    .eq('is_in_scope', true)
    .order('oem_name');

  if (error) throw new Error(error.message);
  return (data ?? []) as OEMInfo[];
}

/**
 * Get data freshness summary.
 */
export async function getDataFreshness(): Promise<FreshnesRow[]> {
  const supabase = createClient();
  const { data, error } = await supabase
    .from('v_data_freshness')
    .select('source,last_attempted,last_success,last_records_loaded,failures_24h');

  if (error) throw new Error(error.message);
  return (data ?? []) as FreshnesRow[];
}
