import { NextResponse } from 'next/server';
import { createClient } from '@/lib/supabase';

export const revalidate = 3600; // ISR: 1 hour

export async function GET() {
  const supabase = createClient();

  // TIV by segment and month (last 13 months for YoY)
  const { data: tiv, error: tivError } = await supabase
    .from('v_monthly_tiv_summary')
    .select('month_key,fy_quarter,segment_code,tiv_units,ev_units,ev_penetration_pct')
    .order('month_key', { ascending: false })
    .limit(52); // ~13 months × 4 segments

  if (tivError) {
    return NextResponse.json({ error: tivError.message }, { status: 500 });
  }

  // OEM market share (current month, top 5 per segment)
  const { data: share, error: shareError } = await supabase
    .from('v_oem_market_share')
    .select('month_key,oem_name,nse_ticker,segment_code,oem_units,market_share_pct')
    .order('month_key', { ascending: false })
    .limit(100);

  if (shareError) {
    return NextResponse.json({ error: shareError.message }, { status: 500 });
  }

  return NextResponse.json({ tiv, share });
}
