import { NextResponse } from 'next/server';
import { createClient } from '@/lib/supabase';

export const revalidate = 3600;

export async function GET() {
  const supabase = createClient();

  const { data, error } = await supabase
    .from('est_quarterly_revenue')
    .select(
      `fy_quarter,
       units_retail,
       units_wholesale,
       asp_used,
       revenue_retail_cr,
       revenue_wholesale_cr,
       data_completeness,
       generated_at`
    )
    .order('fy_quarter', { ascending: false })
    .limit(60); // ~5 years of quarterly data

  if (error) {
    return NextResponse.json({ error: error.message }, { status: 500 });
  }

  return NextResponse.json({ data });
}
