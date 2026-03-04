'use client';

import Link from 'next/link';
import { usePathname } from 'next/navigation';

const SEGMENTS = [
  { code: 'PV', label: 'Passenger Vehicles', color: 'bg-blue-500' },
  { code: 'CV', label: 'Commercial Vehicles', color: 'bg-amber-500' },
  { code: '2W', label: 'Two Wheelers',        color: 'bg-emerald-500' },
];

const OEM_GROUPS = [
  {
    label: 'Passenger Vehicles',
    oems: [
      { ticker: 'MARUTI',     label: 'Maruti Suzuki' },
      { ticker: 'HYUNDAI',    label: 'Hyundai' },
      { ticker: 'TATAMOTORS', label: 'Tata Motors PV' },
      { ticker: 'M&M',        label: 'Mahindra PV' },
    ],
  },
  {
    label: 'Commercial Vehicles',
    oems: [
      { ticker: 'TATAMOTORS', label: 'Tata Motors CV' },
      { ticker: 'ASHOKLEY',   label: 'Ashok Leyland' },
      { ticker: 'EICHERMOT',  label: 'Eicher Motors' },
      { ticker: 'M&M',        label: 'Mahindra CV' },
    ],
  },
  {
    label: 'Two Wheelers',
    oems: [
      { ticker: 'HEROMOTOCO', label: 'Hero MotoCorp' },
      { ticker: 'BAJAJ-AUTO', label: 'Bajaj Auto' },
      { ticker: 'TVSMOTOR',   label: 'TVS Motor' },
      { ticker: 'EICHERMOT',  label: 'Royal Enfield' },
      { ticker: 'OLAELEC',    label: 'Ola Electric' },
    ],
  },
];

export function Sidebar() {
  const pathname = usePathname();

  return (
    <aside className="w-56 shrink-0 border-r border-slate-200 bg-white h-full overflow-y-auto">
      <nav className="p-4 space-y-6">
        {/* Segments */}
        <div>
          <p className="text-xs font-semibold text-slate-400 uppercase tracking-wider mb-2">Segments</p>
          <ul className="space-y-1">
            {SEGMENTS.map(seg => (
              <li key={seg.code}>
                <Link
                  href={`/dashboard?segment=${seg.code}`}
                  className="flex items-center gap-2 px-3 py-2 rounded text-sm text-slate-700 hover:bg-slate-50"
                >
                  <span className={`w-2 h-2 rounded-full ${seg.color}`} />
                  {seg.label}
                </Link>
              </li>
            ))}
          </ul>
        </div>

        {/* OEM Deep Dives */}
        {OEM_GROUPS.map(group => (
          <div key={group.label}>
            <p className="text-xs font-semibold text-slate-400 uppercase tracking-wider mb-2">
              {group.label}
            </p>
            <ul className="space-y-1">
              {group.oems.map(oem => (
                <li key={`${group.label}-${oem.ticker}`}>
                  <Link
                    href={`/oem/${oem.ticker}`}
                    className={`flex items-center px-3 py-1.5 rounded text-sm transition-colors ${
                      pathname === `/oem/${oem.ticker}`
                        ? 'bg-brand-50 text-brand-700 font-medium'
                        : 'text-slate-600 hover:bg-slate-50'
                    }`}
                  >
                    {oem.label}
                  </Link>
                </li>
              ))}
            </ul>
          </div>
        ))}
      </nav>
    </aside>
  );
}
