import type { Metadata } from 'next';
import './globals.css';

export const metadata: Metadata = {
  title: 'AutoQuant — India Auto Registrations Dashboard',
  description:
    'Private data platform for tracking Indian automobile registrations (VAHAN), ' +
    'reconciling against FADA data, and generating demand-based revenue proxies.',
};

export default function RootLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  return (
    <html lang="en">
      <body className="min-h-screen bg-slate-50">
        {/* Top nav */}
        <nav className="bg-white border-b border-slate-200 px-6 py-3 flex items-center gap-6">
          <span className="font-bold text-brand-600 text-lg tracking-tight">
            AutoQuant
          </span>
          <a href="/dashboard" className="text-sm text-slate-600 hover:text-brand-600">Dashboard</a>
          <a href="/revenue"  className="text-sm text-slate-600 hover:text-brand-600">Revenue</a>
          <a href="/scorecard" className="text-sm text-slate-600 hover:text-brand-600">Scorecard</a>
          <a href="/history"  className="text-sm text-slate-600 hover:text-brand-600">History</a>
        </nav>

        {/* Disclaimer */}
        <div className="mx-6 mt-3 disclaimer-banner">
          ⚠️ Revenue figures are demand-based proxies (registrations × ASP assumption).
          NOT accounting revenue. Do not use for investment decisions.
        </div>

        <main className="px-6 py-6">{children}</main>
      </body>
    </html>
  );
}
