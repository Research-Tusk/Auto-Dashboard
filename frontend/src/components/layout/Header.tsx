'use client';

import Link from 'next/link';
import { usePathname } from 'next/navigation';
import { clsx } from 'clsx';

const NAV_ITEMS = [
  { href: '/dashboard', label: 'Dashboard' },
  { href: '/scorecard', label: 'Scorecard' },
  { href: '/history', label: 'History' },
  { href: '/revenue', label: 'Revenue' },
] as const;

export function Header() {
  const pathname = usePathname();

  return (
    <header className="sticky top-0 z-40 bg-white/90 backdrop-blur-sm border-b border-slate-200">
      <div className="max-w-[1440px] mx-auto px-4 sm:px-6 lg:px-8">
        <div className="flex items-center justify-between h-14">
          {/* Logo / brand */}
          <Link href="/dashboard" className="flex items-center gap-2 group">
            <div className="w-7 h-7 rounded-lg bg-brand-600 flex items-center justify-center">
              <span className="text-white text-xs font-bold tracking-tight">AQ</span>
            </div>
            <span className="text-sm font-semibold text-slate-900 tracking-tight">AutoQuant</span>
          </Link>

          {/* Primary nav */}
          <nav className="flex items-center gap-1">
            {NAV_ITEMS.map((item) => (
              <Link
                key={item.href}
                href={item.href}
                className={clsx(
                  'px-3 py-1.5 rounded-lg text-sm font-medium transition-colors',
                  pathname === item.href || pathname.startsWith(item.href + '/')
                    ? 'bg-brand-50 text-brand-700'
                    : 'text-slate-600 hover:text-slate-900 hover:bg-slate-50'
                )}
              >
                {item.label}
              </Link>
            ))}
          </nav>

          {/* Right slot — future: user avatar, dark-mode toggle */}
          <div className="w-24 flex justify-end">
            <span className="text-xs text-slate-400 font-mono">beta</span>
          </div>
        </div>
      </div>
    </header>
  );
}
