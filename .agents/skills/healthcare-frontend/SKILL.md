---
name: healthcare-frontend
description: >-
  Provides runbook patterns, component architectures, routing conventions, and UI design standards
  for the Vite + React + TypeScript + Tailwind CSS price transparency web application.
---

# Healthcare Frontend Architecture & Agent Runbook

This skill documents the conventions, design patterns, and repeated procedures for developing the user interface in `frontend/`.

## Architecture & Technology Stack
- **Framework**: React 18 + TypeScript + Vite
- **Styling**: Tailwind CSS with custom color palette and responsive utilities
- **Data Fetching**: Custom API client (`src/api/client.ts`) and React Query hooks (`src/api/hooks/`)
- **Accessibility**: WCAG 2.1 AA compliant, semantic HTML5, skip navigation link, keyboard navigable

## Directory Structure
```text
frontend/src/
├── api/
│   ├── client.ts              # Fetch-based REST client for /api/v1 endpoints
│   ├── types.ts               # TypeScript data models mirroring Rust common::model
│   └── hooks/                 # Custom React query/fetch hooks
├── components/
│   ├── common/                # Header, Footer, Button, Input, Select, Badge, Skeleton, SkipLink
│   ├── hospitals/             # HospitalCard, HospitalDetailView, HospitalFilterBar, HospitalList
│   ├── procedures/            # ProcedureFilterBar, ProcedureTable
│   ├── compare/               # CompareFilterBar, PriceComparisonTable, PriceSpreadChart, PriceStatSummary
│   └── ui/                    # Dialog, Tabs primitives
└── pages/
    ├── HospitalsPage.tsx      # Hospital search by name, city, state, or license
    ├── ProceduresPage.tsx     # Procedure & standard charge search by code/keywords
    ├── ComparePage.tsx        # Cross-hospital & cross-payer price comparison
    ├── DataSourcesPage.tsx    # Data provenance declaration & local replication runbook
    └── AboutPage.tsx          # Project overview, methodology, data source explanation
```

## Common Developer Patterns for Agents

### 1. Adding a New Page or View
When creating a new page component in `src/pages/`:
1. Export the component function:
   ```tsx
   export function NewFeaturePage() {
     return (
       <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
         <h1 className="text-3xl font-bold text-slate-900 dark:text-white">Feature Title</h1>
         ...
       </div>
     );
   }
   ```
2. Add the route/tab navigation item in `src/App.tsx` and header navigation in `src/components/common/Header.tsx`.

### 2. Calling Backend API Endpoints
Always type requests and responses using models from `src/api/types.ts`:
```tsx
import { searchHospitals, getHospitalById } from '../api/client';
import type { HospitalSummary, HospitalSearchParams } from '../api/types';

// In component or hook:
const [data, setData] = useState<HospitalSummary[]>([]);
const [loading, setLoading] = useState(false);

const handleSearch = async (params: HospitalSearchParams) => {
  setLoading(true);
  try {
    const res = await searchHospitals(params);
    setData(res.items);
  } finally {
    setLoading(false);
  }
};
```

### 3. Responsive Tables & Mobile Support
Following the project's mobile-first convention:
- For desktop screens (`md:` and above), render rich tabular data (`PriceComparisonTable`, `ProcedureTable`).
- For mobile screens, render responsive stacked cards (`PriceCardMobile`, `HospitalCard`) with touch-friendly tap targets (minimum 44x44px).

### 4. Accessibility Conventions (WCAG 2.1 AA)
- Every input must have an associated `<label>` or explicit `aria-label`.
- Interactive elements must support `:focus-visible` outlines.
- Provide a `SkipLink` at the top of `App.tsx` targeting `#main-content`.
- Data tables must include `scope="col"` on header cells.

## Building and Running Frontend

```bash
# Start development server (port 5173 with proxy to backend port 3000)
npm run dev

# Run type check and lint
npm run build
```
