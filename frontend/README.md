# AutoQuant — Frontend Dashboard

Next.js 14 dashboard for India auto registration data and OEM demand proxies.

## Setup

```bash
cp .env.example .env.local
# Edit .env.local: set NEXT_PUBLIC_SUPABASE_URL, NEXT_PUBLIC_SUPABASE_ANON_KEY
npm install
npm run dev
```

## Pages

| Route | Description |
|-------|-------------|
| `/dashboard` | Industry Pulse — TIV trends, EV mix, segment overview |
| `/oem/[ticker]` | OEM Deep Dive — market share, MTD trend, EV mix |
| `/revenue` | Revenue Estimator — quarterly demand proxy |
| `/scorecard` | Quarterly Scorecard — OEM vs consensus |
| `/history` | Historical Explorer — multi-year trends |

## Structure

```
frontend/
├── app/                         # Next.js 14 App Router
│   ├── layout.tsx               # Root layout
│   ├── page.tsx                 # Root redirect → /dashboard
│   ├── dashboard/
│   │   └── page.tsx             # Industry Pulse dashboard
│   ├── oem/
│   │   └── [ticker]/
│   │       └── page.tsx         # OEM Deep Dive
│   ├── revenue/
│   │   └── page.tsx             # Revenue Estimator
│   ├── scorecard/
│   │   └── page.tsx             # Quarterly Scorecard
│   ├── history/
│   │   └── page.tsx             # Historical Explorer
│   └── api/
│       ├── tiv/route.ts         # Industry TIV API
│       ├── oem/[ticker]/route.ts # OEM data API
│       ├── revenue/route.ts     # Revenue proxy API
│       └── revalidate/route.ts  # ISR revalidation webhook
├── src/
│   ├── components/
│   │   ├── charts/              # Recharts wrappers
│   │   ├── tables/              # Data tables
│   │   └── cards/               # KPI cards
│   ├── lib/
│   │   ├── supabase.ts          # Supabase client
│   │   ├── queries.ts           # DB query functions
│   │   └── formatters.ts        # Number/date formatters
│   └── types/
│       └── index.ts             # TypeScript interfaces
├── package.json
├── tailwind.config.js
├── next.config.js
├── tsconfig.json
└── vercel.json
```

## Tech Stack

| Technology | Version | Purpose |
|------------|---------|----------|
| Next.js | 14.x | App Router, ISR, API routes |
| React | 18.x | UI |
| TypeScript | 5.x | Type safety |
| Tailwind CSS | 3.x | Styling |
| shadcn/ui | latest | UI components |
| Recharts | 2.x | Charts |
| @supabase/ssr | latest | Supabase client |

## Data Fetching Strategy

- **ISR (Incremental Static Regeneration)**: Historical data pages (revalidate: 3600s)
- **SSR (Server-Side Rendering)**: Current month / live data pages
- **Revalidation webhook**: ETL triggers `POST /api/revalidate` after each successful load

## Deployment (Vercel)

```bash
npm run build  # Verify build passes before deploying
vercel --prod  # Deploy
```

Environment variables required in Vercel dashboard:
- `NEXT_PUBLIC_SUPABASE_URL`
- `NEXT_PUBLIC_SUPABASE_ANON_KEY`
- `NEXT_REVALIDATE_SECRET`
