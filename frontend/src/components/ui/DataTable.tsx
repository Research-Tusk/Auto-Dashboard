'use client';

import { useState, type ReactNode } from 'react';
import { clsx } from 'clsx';

export interface Column<T> {
  key: keyof T & string;
  label: string;
  sortable?: boolean;
  align?: 'left' | 'right' | 'center';
  render?: (row: T) => ReactNode;
}

interface DataTableProps<T> {
  columns: Column<T>[];
  data: T[];
  rowKey: (row: T) => string;
  defaultSortKey?: keyof T & string;
  defaultSortDir?: 'asc' | 'desc';
  emptyMessage?: string;
  maxRows?: number;
}

export function DataTable<T>({
  columns,
  data,
  rowKey,
  defaultSortKey,
  defaultSortDir = 'asc',
  emptyMessage = 'No data',
  maxRows,
}: DataTableProps<T>) {
  const [sortKey, setSortKey] = useState<keyof T & string | undefined>(defaultSortKey);
  const [sortDir, setSortDir] = useState<'asc' | 'desc'>(defaultSortDir);

  const handleSort = (key: keyof T & string) => {
    if (sortKey === key) {
      setSortDir(d => (d === 'asc' ? 'desc' : 'asc'));
    } else {
      setSortKey(key);
      setSortDir('asc');
    }
  };

  const sorted = [...data].sort((a, b) => {
    if (!sortKey) return 0;
    const av = a[sortKey];
    const bv = b[sortKey];
    if (av == null && bv == null) return 0;
    if (av == null) return 1;
    if (bv == null) return -1;
    const cmp = av < bv ? -1 : av > bv ? 1 : 0;
    return sortDir === 'asc' ? cmp : -cmp;
  });

  const visible = maxRows ? sorted.slice(0, maxRows) : sorted;

  if (data.length === 0) {
    return (
      <div className="py-10 text-center text-sm text-slate-400">{emptyMessage}</div>
    );
  }

  return (
    <div className="overflow-x-auto -mx-1">
      <table className="min-w-full text-sm">
        <thead>
          <tr className="border-b border-slate-100">
            {columns.map((col) => (
              <th
                key={col.key}
                className={clsx(
                  'px-3 py-2 text-xs font-semibold text-slate-500 whitespace-nowrap',
                  col.align === 'right' ? 'text-right' : col.align === 'center' ? 'text-center' : 'text-left',
                  col.sortable ? 'cursor-pointer select-none hover:text-slate-900' : ''
                )}
                onClick={col.sortable ? () => handleSort(col.key) : undefined}
              >
                {col.label}
                {col.sortable && sortKey === col.key && (
                  <span className="ml-1 text-brand-500">
                    {sortDir === 'asc' ? '↑' : '↓'}
                  </span>
                )}
              </th>
            ))}
          </tr>
        </thead>
        <tbody className="divide-y divide-slate-50">
          {visible.map((row) => (
            <tr key={rowKey(row)} className="hover:bg-slate-50/60 transition-colors">
              {columns.map((col) => (
                <td
                  key={col.key}
                  className={clsx(
                    'px-3 py-2.5 text-slate-700 whitespace-nowrap',
                    col.align === 'right' ? 'text-right' : col.align === 'center' ? 'text-center' : 'text-left'
                  )}
                >
                  {col.render ? col.render(row) : String(row[col.key] ?? '—')}
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
      {maxRows && sorted.length > maxRows && (
        <p className="mt-2 text-center text-xs text-slate-400">
          Showing {maxRows} of {sorted.length} rows
        </p>
      )}
    </div>
  );
}
