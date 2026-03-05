import { NextResponse } from 'next/server';
import { createServerSupabaseClient } from '@/lib/supabase-server';
import type { OEMSummaryRow, RevenueRow, OEMShareRow } from '@/types';

interface RouteParams {
  params: { ticker: string };
}

export async function GET(_request: Request, { params }: RouteParams) {
  const { ticker } = params;
  if (!ticker) {
    return NextResponse.json({ error: 'Missing ticker' }, { status: 400 });
  }

  const upperTicker = ticker.toUpperCase();

  try {
    const supabase = createServerSupabaseClient();

    // Monthly summary from materialized view
    const { data: monthly, error: monthlyError } = await supabase
      .from('mv_oem_monthly_summary')
      .select(
        'month_key,fy_year,fy_quarter,oem_name,segment_code,fuel_bucket,total_units,units_prior_year,last_updated'
      )
      .eq('nse_ticker', upperTicker)
      .order('month_key', { ascending: false })
      .limit(156); // 13 months × 12 segments/fuels

    if (monthlyError) {
      return NextResponse.json({ error: monthlyError.message }, { status: 500 });
    }

    // Revenue proxy — est_quarterly_revenue fetched for all OEMs (join to ticker done in view)
    const { data: revenue, error: revenueError } = await supabase
      .from('est_quarterly_revenue')
      .select(
        'fy_quarter,oem_id,segment_id,units_retail,units_wholesale,asp_used,revenue_retail_cr,revenue_wholesale_cr,data_completeness,generated_at'
      )
      .order('fy_quarter', { ascending: false })
      .limit(80);

    if (revenueError) {
      return NextResponse.json({ error: revenueError.message }, { status: 500 });
    }

    // Market share for this OEM
    const { data: share, error: shareError } = await supabase
      .from('v_oem_market_share')
      .select('month_key,oem_name,nse_ticker,segment_code,oem_units,segment_tiv,market_share_pct')
      .eq('nse_ticker', upperTicker)
      .order('month_key', { ascending: false })
      .limit(78); // 13 months × 6 segments

    if (shareError) {
      return NextResponse.json({ error: shareError.message }, { status: 500 });
    }

    return NextResponse.json({
      monthly: (monthly ?? []) as OEMSummaryRow[],
      revenue: (revenue ?? []) as RevenueRow[],
      share: (share ?? []) as OEMShareRow[],
    });
  } catch (err) {
    console.error('OEM API error:', err);
    return NextResponse.json(
      { error: 'Internal server error' },
      { status: 500 }
    );
  }

}
