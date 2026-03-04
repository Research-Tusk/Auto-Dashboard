import { NextRequest, NextResponse } from 'next/server';
import { createClient } from '@/lib/supabase';

export const revalidate = 3600;

export async function GET(request: NextRequest) {
  const { searchParams } = new URL(request.url);
  const segment = searchParams.get('segment') || 'PV';
  const fromYear = parseInt(searchParams.get('from_year') || '2020', 10);
  const toYear = parseInt(searchParams.get('to_year') || new Date().getFullYear().toString(), 10);

  const supabase = createClient();

  // Monthly TIV history
  const { data: tiv, error: tivError } = await supabase
    .from('v_monthly_tiv_summary')
    .select('month_key,fy_quarter,tiv_units,ev_units,ev_penetration_pct')
    .eq('segment_code', segment.toUpperCase())
    .gte('month_key', `${fromYear}-01-01`)
    .lte('month_key', `${toYear}-12-31`)
    .order('month_key', { ascending: true });

  if (tivError) {
    return NextResponse.json({ error: tivError.message }, { status: 500 });
  }

  // OEM market share history (annual)
  const { data: share, error: shareError } = await supabase
    .from('v_oem_market_share')
    .select('month_key,oem_name,nse_ticker,oem_units,market_share_pct')
    .eq('segment_code', segment.toUpperCase())
    .gte('month_key', `${fromYear}-01-01`)
    .lte('month_key', `${toYear}-12-31`)
    .order('month_key', { ascending: true })
    .limit(2000);

  if (shareError) {
    return NextResponse.json({ error: shareError.message }, { status: 500 });
  }

  // EV penetration quarterly
  const { data: evPen, error: evError } = await supabase
    .from('v_monthly_tiv_summary')
    .select('fy_quarter,ev_penetration_pct')
    .eq('segment_code', segment.toUpperCase())
    .gte('month_key', `${fromYear}-01-01`)
    .order('month_key', { ascending: true });

  if (evError) {
    return NextResponse.json({ error: evError.message }, { status: 500 });
  }

  return NextResponse.json({ tiv, share, evPen });
}
