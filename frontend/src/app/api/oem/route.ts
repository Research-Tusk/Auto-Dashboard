import { NextRequest, NextResponse } from 'next/server';
import { createClient } from '@/lib/supabase';

export const revalidate = 3600;

export async function GET(request: NextRequest) {
  const { searchParams } = new URL(request.url);
  const ticker = searchParams.get('ticker');

  if (!ticker) {
    return NextResponse.json({ error: 'ticker is required' }, { status: 400 });
  }

  const supabase = createClient();

  // OEM monthly summary from MV
  const { data: monthly, error: monthlyError } = await supabase
    .from('mv_oem_monthly_summary')
    .select(
      'month_key,fy_year,fy_quarter,oem_name,segment_code,fuel_bucket,total_units,units_prior_year,last_updated'
    )
    .eq('nse_ticker', ticker.toUpperCase())
    .order('month_key', { ascending: false })
    .limit(156); // 13 months × 12 dimension combos

  if (monthlyError) {
    return NextResponse.json({ error: monthlyError.message }, { status: 500 });
  }

  // Revenue proxy
  const { data: revenue, error: revenueError } = await supabase
    .from('est_quarterly_revenue')
    .select(
      'fy_quarter,oem_id,segment_code:segment_id,units_retail,asp_used,revenue_retail_cr,data_completeness'
    )
    .order('fy_quarter', { ascending: false })
    .limit(20);

  if (revenueError) {
    return NextResponse.json({ error: revenueError.message }, { status: 500 });
  }

  // Market share (last 6 months)
  const { data: share, error: shareError } = await supabase
    .from('v_oem_market_share')
    .select('month_key,oem_name,segment_code,oem_units,segment_tiv,market_share_pct')
    .eq('nse_ticker', ticker.toUpperCase())
    .order('month_key', { ascending: false })
    .limit(36);

  if (shareError) {
    return NextResponse.json({ error: shareError.message }, { status: 500 });
  }

  return NextResponse.json({ monthly, revenue, share });
}
