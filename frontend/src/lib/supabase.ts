import { createBrowserClient } from '@supabase/ssr';

/**
 * Create a Supabase client for use in browser (client components).
 * Uses NEXT_PUBLIC_* env vars which are safe to expose to the browser.
 */
export function createClient() {
  return createBrowserClient(
    process.env.NEXT_PUBLIC_SUPABASE_URL!,
    process.env.NEXT_PUBLIC_SUPABASE_ANON_KEY!
  );
}

/**
 * Type helper for Supabase tables used in AutoQuant.
 * Extend as more tables are added to the query layer.
 */
export type SupabaseClient = ReturnType<typeof createClient>;
