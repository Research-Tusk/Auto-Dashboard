/**
 * AutoQuant — Supabase Query Functions
 * Typed query helpers for each dashboard section.
 * Uses supabase-server (RSC) or supabase-client (CSR) depending on context.
 */

import { createServerSupabase } from './supabase-server';
import type {
  OEMRecord,
  TIVRecord,
  OEMScorecard,
  MSHistoryRecord,
  RevenueRow,
  PowertrainRow,
} from '@/types';

// ---------------------------------------------------------------------------
// Dimension tables
// ---------------------------------------------------------------------------

export async function fetchOEMs(): Promise<OEMRecord[]> {
  const supabase = await createServerSupabase();
  const { data, error } = await supabase
    .from('dim_oem')
    .select('oem_id, oem_name, nse_ticker, primary_segments, is_in_scope, description')
    .eq('is_in_scope', true)
    .order('oem_name');
  if (error) throw new Error(`fetchOEMs: ${error.message}`);
  return (data ?? []) as OEMRecord[];
}

export async function fetchOEMByTicker(ticker: string): Promise<OEMRecord | null> {
  const supabase = await createServerSupabase();
  const { data, error } = await supabase
    .from('dim_oem')
    .select('oem_id, oem_name, nse_ticker, primary_segments, is_in_scope, description')
    .eq('nse_ticker', ticker.toUpperCase())
    .single();
  if (error) return null;
  return data as OEMRecord;
}

// ---------------------------------------------------------------------------
// TIV (Total Industry Volume)
// ---------------------------------------------------------------------------

export async function fetchTIV(segmentId?: number): Promise<TIVRecord[]> {
  const supabase = await createServerSupabase();
  let query = supabase
    .from('fact_tiv')
    .select('fy_quarter, segment_id, tiv_retail, tiv_wholesale, tiv_yoy_pct')
    .order('fy_quarter', { ascending: true })
    .limit(80);
  if (segmentId != null) query = query.eq('segment_id', segmentId);
  const { data, error } = await query;
  if (error) throw new Error(`fetchTIV: ${error.message}`);
  return (data ?? []) as TIVRecord[];
}

// ---------------------------------------------------------------------------
// Scorecard
// ---------------------------------------------------------------------------

export async function fetchScorecard(
  segmentId: number,
  quarter?: string
): Promise<OEMScorecard[]> {
  const supabase = await createServerSupabase();
  let query = supabase
    .from('oem_scorecard')
    .select('*')
    .eq('segment_id', segmentId)
    .order('ms_pct', { ascending: false });
  if (quarter) query = query.eq('fy_quarter', quarter);
  const { data, error } = await query;
  if (error) throw new Error(`fetchScorecard: ${error.message}`);
  return (data ?? []) as OEMScorecard[];
}

// ---------------------------------------------------------------------------
// Market Share History
// ---------------------------------------------------------------------------

export async function fetchMSHistory(
  segmentId: number,
  oemId?: number
): Promise<MSHistoryRecord[]> {
  const supabase = await createServerSupabase();
  let query = supabase
    .from('oem_ms_history')
    .select('fy_quarter, oem_id, segment_id, units_retail, ms_pct, ms_pct_qoq, ms_pct_yoy')
    .eq('segment_id', segmentId)
    .order('fy_quarter', { ascending: true })
    .limit(200);
  if (oemId != null) query = query.eq('oem_id', oemId);
  const { data, error } = await query;
  if (error) throw new Error(`fetchMSHistory: ${error.message}`);
  return (data ?? []) as MSHistoryRecord[];
}

// ---------------------------------------------------------------------------
// Revenue estimates
// ---------------------------------------------------------------------------

export async function fetchRevenue(
  segmentId?: number,
  oemId?: number,
  limit = 200
): Promise<RevenueRow[]> {
  const supabase = await createServerSupabase();
  let query = supabase
    .from('est_quarterly_revenue')
    .select(
      'fy_quarter, oem_id, segment_id, units_retail, units_wholesale, asp_used, ' +
      'revenue_retail_cr, revenue_wholesale_cr, data_completeness, generated_at'
    )
    .order('fy_quarter', { ascending: false })
    .limit(limit);
  if (segmentId != null) query = query.eq('segment_id', segmentId);
  if (oemId != null) query = query.eq('oem_id', oemId);
  const { data, error } = await query;
  if (error) throw new Error(`fetchRevenue: ${error.message}`);
  return (data ?? []) as RevenueRow[];
}

// ---------------------------------------------------------------------------
// Powertrain mix
// ---------------------------------------------------------------------------

export async function fetchPowertrain(
  segmentId?: number,
  oemId?: number
): Promise<PowertrainRow[]> {
  const supabase = await createServerSupabase();
  let query = supabase
    .from('fact_powertrain')
    .select('fy_quarter, oem_id, segment_id, powertrain, units_retail, share_pct')
    .order('fy_quarter', { ascending: true })
    .limit(300);
  if (segmentId != null) query = query.eq('segment_id', segmentId);
  if (oemId != null) query = query.eq('oem_id', oemId);
  const { data, error } = await query;
  if (error) throw new Error(`fetchPowertrain: ${error.message}`);
  return (data ?? []) as PowertrainRow[];
}

// ---------------------------------------------------------------------------
// Dashboard summary (used by API route)
// ---------------------------------------------------------------------------

export async function fetchDashboardSummary(segmentId: number) {
  const [tiv, scorecard] = await Promise.all([
    fetchTIV(segmentId),
    fetchScorecard(segmentId),
  ]);

  const latestTIVQuarter = tiv.length > 0 ? tiv[tiv.length - 1].fy_quarter : null;
  const latestTIV = tiv.length > 0 ? tiv[tiv.length - 1] : null;

  const latestScorecardQuarter = scorecard.length > 0
    ? [...new Set(scorecard.map(s => s.fy_quarter))].sort().reverse()[0]
    : null;
  const latestScorecard = scorecard.filter(s => s.fy_quarter === latestScorecardQuarter);

  return {
    latestTIVQuarter,
    latestTIV,
    latestScorecardQuarter,
    topOEMs: latestScorecard.slice(0, 5),
    tivHistory: tiv,
  };
}
