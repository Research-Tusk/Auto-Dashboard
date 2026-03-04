import { NextResponse } from 'next/server';
import { createClient } from '@/lib/supabase';

export const revalidate = 3600;

export async function GET() {
  const supabase = createClient();

  // Get last 8 quarters of revenue data for scorecard
  const { data: revenue, error: revenueError } = await supabase
    .from('est_quarterly_revenue')
    .select(
      `fy_quarter,
       oem_id,
       segment_id,
       units_retail,
       asp_used,
       revenue_retail_cr,
       data_completeness,
       generated_at`
    )
    .order('fy_quarter', { ascending: false })
    .limit(200);

  if (revenueError) {
    return NextResponse.json({ error: revenueError.message }, { status: 500 });
  }

  // OEM names for display
  const { data: oems, error: oemsError } = await supabase
    .from('dim_oem')
    .select('oem_id,oem_name,nse_ticker')
    .eq('is_in_scope', true)
    .order('oem_name');

  if (oemsError) {
    return NextResponse.json({ error: oemsError.message }, { status: 500 });
  }

  return NextResponse.json({ revenue, oems });
}
