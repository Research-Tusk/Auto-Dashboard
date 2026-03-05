import type { Metadata } from 'next';
import { Inter } from 'next/font/google';
import './globals.css';
import { Header } from '@/components/layout/Header';

const inter = Inter({
  subsets: ['latin'],
  variable: '--font-inter',
  display: 'swap',
});

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
    <html lang="en" className={inter.variable}>
      <body className="min-h-screen bg-slate-50 antialiased">
        <Header />
        {/* Revenue disclaimer — always visible */}
        <div className="mx-4 sm:mx-6 mt-3 disclaimer-banner flex items-start gap-2">
          <span className="shrink-0 mt-0.5">⚠️</span>
          <span>
            <strong>Research use only.</strong> Revenue figures are demand-based proxies
            (registrations × ASP assumption). NOT accounting revenue. Not for investment decisions.
          </span>
        </div>
        <main className="px-4 sm:px-6 py-6">{children}</main>
      </body>
    </html>
  );
}
