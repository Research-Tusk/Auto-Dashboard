'use client';

import Link from 'next/link';
import { usePathname } from 'next/navigation';

const NAV_LINKS = [
  { href: '/dashboard', label: 'Dashboard', icon: '📈' },
  { href: '/revenue',   label: 'Revenue',   icon: '💰' },
  { href: '/scorecard', label: 'Scorecard', icon: '🏆' },
  { href: '/history',   label: 'History',   icon: '🗂️' },
];

const OEM_LINKS = [
  { ticker: 'MARUTI',     label: 'Maruti Suzuki' },
  { ticker: 'HYUNDAI',    label: 'Hyundai' },
  { ticker: 'TATAMOTORS', label: 'Tata Motors' },
  { ticker: 'M&M',        label: 'Mahindra' },
  { ticker: 'HEROMOTOCO', label: 'Hero MotoCorp' },
  { ticker: 'BAJAJ-AUTO', label: 'Bajaj Auto' },
  { ticker: 'TVSMOTOR',   label: 'TVS Motor' },
  { ticker: 'EICHERMOT',  label: 'Eicher (RE/CV)' },
  { ticker: 'ASHOKLEY',   label: 'Ashok Leyland' },
  { ticker: 'OLAELEC',    label: 'Ola Electric' },
];

export function Header() {
  const pathname = usePathname();

  return (
    <header className="bg-white border-b border-slate-200 sticky top-0 z-50">
      <div className="max-w-screen-2xl mx-auto px-6 py-3 flex items-center gap-6">
        {/* Logo */}
        <Link href="/dashboard" className="font-bold text-brand-600 text-lg tracking-tight shrink-0">
          AutoQuant
        </Link>

        {/* Main nav */}
        <nav className="flex items-center gap-1">
          {NAV_LINKS.map(link => (
            <Link
              key={link.href}
              href={link.href}
              className={`px-3 py-1.5 rounded text-sm font-medium transition-colors ${
                pathname === link.href
                  ? 'bg-brand-50 text-brand-700'
                  : 'text-slate-600 hover:text-brand-600 hover:bg-slate-50'
              }`}
            >
              {link.label}
            </Link>
          ))}
        </nav>

        {/* OEM quick links */}
        <div className="flex items-center gap-1 ml-4 border-l border-slate-200 pl-4">
          <span className="text-xs text-slate-400 font-medium mr-1">OEM:</span>
          {OEM_LINKS.slice(0, 5).map(oem => (
            <Link
              key={oem.ticker}
              href={`/oem/${oem.ticker}`}
              className={`px-2 py-1 rounded text-xs font-medium transition-colors ${
                pathname === `/oem/${oem.ticker}`
                  ? 'bg-brand-50 text-brand-700'
                  : 'text-slate-500 hover:text-brand-600 hover:bg-slate-50'
              }`}
            >
              {oem.ticker}
            </Link>
          ))}
        </div>

        {/* Disclaimer pill */}
        <div className="ml-auto text-xs text-amber-600 bg-amber-50 border border-amber-200 rounded-full px-3 py-1 shrink-0">
          ⚠️ Demand proxies only — not accounting revenue
        </div>
      </div>
    </header>
  );
}
