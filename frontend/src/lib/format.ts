/**
 * AutoQuant — Frontend Formatters
 * Utility functions for formatting numbers, dates, and currency values.
 */

/**
 * Format a number as Indian currency (INR Crore with commas).
 * e.g. 125000 → '₹1,25,000 Cr'
 */
export function formatCrore(value: number | null | undefined): string {
  if (value == null) return '—';
  return `₹${value.toLocaleString('en-IN', { maximumFractionDigits: 0 })} Cr`;
}

/**
 * Format units with K/M suffix.
 * e.g. 125000 → '1.25L' (India convention) or '125K'
 */
export function formatUnits(value: number | null | undefined, style: 'K' | 'L' | 'M' = 'K'): string {
  if (value == null) return '—';
  if (style === 'L') return `${(value / 100_000).toFixed(2)}L`;
  if (style === 'M') return `${(value / 1_000_000).toFixed(2)}M`;
  if (value >= 1_000_000) return `${(value / 1_000_000).toFixed(1)}M`;
  if (value >= 1_000) return `${(value / 1_000).toFixed(1)}K`;
  return value.toString();
}

/**
 * Format a percentage.
 * e.g. 15.23 → '15.2%'
 */
export function formatPct(value: number | null | undefined, decimals = 1): string {
  if (value == null) return '—';
  return `${value.toFixed(decimals)}%`;
}

/**
 * Format a date string as 'MMM YYYY'.
 * e.g. '2025-12-01' → 'Dec 2025'
 */
export function formatMonth(dateStr: string | null | undefined): string {
  if (!dateStr) return '—';
  const d = new Date(dateStr);
  return d.toLocaleDateString('en-IN', { month: 'short', year: 'numeric' });
}

/**
 * Format ASP in INR Lakhs.
 * e.g. 12.5 → '₹12.5L'
 */
export function formatASP(value: number | null | undefined): string {
  if (value == null) return '—';
  return `₹${value.toFixed(2)}L`;
}

/**
 * Compute YoY change percentage.
 */
export function computeYoY(current: number, prior: number): number | null {
  if (!prior || prior === 0) return null;
  return ((current - prior) / prior) * 100;
}

/**
 * Format YoY change as string with sign.
 * e.g. 5.2 → '+5.2%', -3.1 → '-3.1%'
 */
export function formatYoY(value: number | null | undefined): string {
  if (value == null) return '—';
  const sign = value >= 0 ? '+' : '';
  return `${sign}${value.toFixed(1)}%`;
}

/**
 * Truncate OEM name for display.
 * e.g. 'Maruti Suzuki India Ltd' → 'Maruti Suzuki'
 */
export function truncateOEMName(name: string, maxLen = 20): string {
  if (name.length <= maxLen) return name;
  return name.slice(0, maxLen - 1) + '…';
}
