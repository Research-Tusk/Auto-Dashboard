/**
 * AutoQuant — Formatting Utilities
 * Consistent number and date formatting for finance data.
 */

// ---------------------------------------------------------------------------
// Currency helpers
// ---------------------------------------------------------------------------

/** Format a value in Indian Crores. Returns '—' for nullish. */
export function formatCrores(value: number | null | undefined): string {
  if (value == null) return '—';
  return `₹${value.toLocaleString('en-IN', { maximumFractionDigits: 1 })} Cr`;
}

/** Format a value in Indian Lakhs (for ASP). Returns '—' for nullish. */
export function formatLakhs(value: number | null | undefined): string {
  if (value == null) return '—';
  // ASP stored in INR; convert to Lakhs
  const lakhs = value / 1_00_000;
  return `${lakhs.toLocaleString('en-IN', { minimumFractionDigits: 1, maximumFractionDigits: 2 })}L`;
}

// ---------------------------------------------------------------------------
// Unit helpers
// ---------------------------------------------------------------------------

/** Format a vehicle unit count. Returns '—' for nullish. */
export function formatUnits(value: number | null | undefined): string {
  if (value == null) return '—';
  return value.toLocaleString('en-IN');
}

/** Axis tick formatter — compact (e.g. 1.2L, 45K). */
export function formatAxisUnits(value: number): string {
  if (value >= 1_00_000) return `${(value / 1_00_000).toFixed(1)}L`;
  if (value >= 1_000) return `${(value / 1_000).toFixed(0)}K`;
  return String(value);
}

// ---------------------------------------------------------------------------
// Percentage helpers
// ---------------------------------------------------------------------------

/**
 * Format a decimal ratio as a percentage string (e.g. 0.234 → "23.4%").
 * Returns '—' for nullish.
 */
export function formatPct(value: number | null | undefined): string {
  if (value == null) return '—';
  return `${(value * 100).toFixed(1)}%`;
}

/**
 * Format an already-scaled percentage (e.g. 23.4 → "23.4%").
 * Returns '—' for nullish.
 */
export function formatPctPlain(value: number | null | undefined): string {
  if (value == null) return '—';
  return `${value.toFixed(1)}%`;
}

/**
 * Format a percentage change with sign (e.g. +2.3pp or -1.1pp).
 * Input is decimal (0.023 = 2.3pp).
 */
export function formatPpChange(value: number | null | undefined): string {
  if (value == null) return '—';
  const pp = value * 100;
  return `${pp >= 0 ? '+' : ''}${pp.toFixed(1)}pp`;
}

// ---------------------------------------------------------------------------
// Date / quarter helpers
// ---------------------------------------------------------------------------

/**
 * Parse a fiscal quarter string like "FY2425Q3" → human label "FY25 Q3".
 */
export function formatFYQuarter(q: string): string {
  // Expected: FY2425Q3 or FY24Q3 (various conventions)
  const m = q.match(/FY(\d{2,4})(Q\d)/i);
  if (!m) return q;
  const fy = m[1].length === 4 ? m[1].slice(2) : m[1]; // last 2 digits
  return `FY${fy} ${m[2].toUpperCase()}`;
}

/**
 * Sort comparator for fiscal quarter strings (ascending).
 */
export function sortFYQuarter(a: string, b: string): number {
  return a.localeCompare(b);
}
