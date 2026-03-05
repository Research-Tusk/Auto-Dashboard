import { NextResponse } from 'next/server';
import { createServerSupabaseClient } from '@/lib/supabase-server';
import type { TIVRow, OEMShareRow } from '@/types';

export async function GET() {
  try {
    const supabase = createServerSupabaseClient();

    // Fetch last 13 months × all segments
    const { data: tivData, error: tivError } = await supabase
      .from('v_monthly_tiv_summary')
      .select('month_key,fy_quarter,segment_code,tiv_units,ev_units,ev_penetration_pct')
      .order('month_key', { ascending: false })
      .limit(52); // 13 months × ~4 segments

    if (tivError) {
      console.error('TIV query error:', tivError);
      return NextResponse.json({ error: tivError.message }, { status: 500 });
    }

    // Fetch OEM market share — last 6 months for PV
    const { data: shareData, error: shareError } = await supabase
      .from('v_oem_market_share')
      .select('month_key,oem_name,nse_ticker,segment_code,oem_units,segment_tiv,market_share_pct')
      .order('month_key', { ascending: false })
      .limit(360); // 6 months × ~20 OEMs × 3 segments

    if (shareError) {
      console.error('Share query error:', shareError);
      return NextResponse.json({ error: shareError.message }, { status: 500 });
    }

    return NextResponse.json({
      tiv: (tivData ?? []) as TIVRow[],
      share: (shareData ?? []) as OEMShareRow[],
    });
  } catch (err) {
    console.error('Dashboard API error:', err);
    return NextResponse.json(
      { error: 'Internal server error' },
      { status: 500 }
    );
  }
}
