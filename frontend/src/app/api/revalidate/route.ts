import { NextRequest, NextResponse } from 'next/server';

export async function POST(request: NextRequest) {
  const secret = process.env.NEXT_REVALIDATE_SECRET;

  if (!secret) {
    return NextResponse.json(
      { error: 'Revalidation not configured' },
      { status: 500 }
    );
  }

  const { searchParams } = new URL(request.url);
  const token = searchParams.get('secret');

  if (token !== secret) {
    return NextResponse.json({ error: 'Invalid token' }, { status: 401 });
  }

  // Revalidate all dashboard paths
  const { revalidatePath } = await import('next/cache');
  const paths = [
    '/dashboard',
    '/revenue',
    '/scorecard',
    '/history',
  ];

  for (const path of paths) {
    revalidatePath(path);
  }

  return NextResponse.json({
    revalidated: true,
    paths,
    timestamp: new Date().toISOString(),
  });
}
