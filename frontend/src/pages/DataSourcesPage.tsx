import React, { useState } from 'react';
import {
  Database,
  Terminal,
  FileSpreadsheet,
  Layers,
  Copy,
  Check,
  Cpu,
  BookOpen,
} from 'lucide-react';
import { Badge } from '../components/common/Badge';
import { Button } from '../components/common/Button';

export const DataSourcesPage: React.FC = () => {
  const [copiedIndex, setCopiedIndex] = useState<number | null>(null);

  const copyToClipboard = (text: string, index: number) => {
    navigator.clipboard.writeText(text);
    setCopiedIndex(index);
    setTimeout(() => setCopiedIndex(null), 2500);
  };

  const replicationSteps = [
    {
      title: '1. Install Prerequisites',
      desc: 'Ensure DuckDB >= 1.5, Rust (1.80+), and Node.js (v20+) are installed.',
      code: `# DuckDB 1.5+ is required for DuckLake 1.0 format
duckdb --version

# Verify Rust & Cargo
cargo --version

# Verify Node.js & npm
node --version
npm --version`,
    },
    {
      title: '2. Acquire & Extract the DuckLake Dataset',
      desc: 'Download the DuckLake snapshot archive (e.g. mrf_lake_*.zip, ~74GB compressed) from oria-data.trillianthealth.com, then extract into lake/ (never committed to git).',
      code: `# 1. Download snapshot from Trilliant Health (oria-data.trillianthealth.com)
# 2. Extract into the lake/ directory:
unzip mrf_lake_20260721.zip -d lake/

# Verify directory contains metadata.ducklake, catalog.duckdb, and data/
ls -la lake/`,
    },
    {
      title: '3. Mount & Inspect with DuckDB CLI',
      desc: 'Attach the DuckLake storage format directly in DuckDB and query active hospitals.',
      code: `# Open interactive session using the provided initialization script
cd lake
duckdb -readonly -init open-lake.sql catalog.duckdb

# Or attach manually in DuckDB CLI:
INSTALL ducklake; LOAD ducklake;
ATTACH 'ducklake:metadata.ducklake' AS lake (DATA_PATH 'data', OVERRIDE_DATA_PATH true, READ_ONLY);

# Test querying deduplicated hospitals
SELECT COUNT(*) FROM current_hospitals;`,
    },
    {
      title: '4. Start the Rust Backend Engine',
      desc: 'Run the Axum REST backend on Port 3000 pointing to your extracted DuckLake.',
      code: `# From repository root:
cargo run -p backend -- --lake-dir ./lake

# Verify API health in another terminal:
curl http://localhost:3000/api/v1/stats`,
    },
    {
      title: '5. Launch the Web Interface',
      desc: 'Start the Vite development server with proxy forwarding to Port 3000.',
      code: `cd frontend
npm install
npm run dev

# Open http://localhost:5173 in your browser`,
    },
  ];

  return (
    <div className="space-y-8 max-w-6xl mx-auto w-full text-left">
      {/* Header Banner */}
      <div>
        <div className="flex items-center gap-2 mb-2">
          <Badge variant="brand" size="md">
            Provenance & Open Data
          </Badge>
          <Badge variant="success" size="md">
            Reproducible Research
          </Badge>
        </div>
        <h1 className="text-3xl sm:text-4xl font-extrabold text-slate-900 dark:text-slate-100 tracking-tight">
          Data Sources & Replication Guide
        </h1>
        <p className="mt-2 text-sm sm:text-base text-slate-600 dark:text-slate-400 leading-relaxed max-w-3xl">
          CarePrice is committed to open data and reproducible research. Below is the complete declaration of where our healthcare pricing records originate, along with a step-by-step guide on how any researcher or engineer can independently replicate the dataset and local runtime.
        </p>
      </div>

      {/* Declared Data Sources Grid */}
      <div>
        <h2 className="text-lg font-bold text-slate-900 dark:text-slate-100 mb-4 flex items-center gap-2">
          <Database className="h-5 w-5 text-brand-600 dark:text-brand-400" />
          Primary Data Sources & Provenance
        </h2>

        <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
          {/* Source 1: Hospital MRF DuckLake */}
          <div className="rounded-2xl border border-slate-200 dark:border-slate-800 bg-white dark:bg-slate-900 p-6 flex flex-col justify-between shadow-sm">
            <div className="space-y-3">
              <div className="h-10 w-10 rounded-xl bg-brand-50 dark:bg-brand-950 text-brand-600 dark:text-brand-400 flex items-center justify-center">
                <Layers className="h-5 w-5" />
              </div>
              <div>
                <span className="text-xs font-semibold text-brand-600 dark:text-brand-400 uppercase tracking-wider block">
                  Core Pricing Engine
                </span>
                <h3 className="text-base font-bold text-slate-900 dark:text-slate-100 mt-0.5">
                  Hospital Price Transparency DuckLake
                </h3>
              </div>
              <p className="text-xs text-slate-600 dark:text-slate-400 leading-relaxed">
                Aggregated from thousands of hospital Machine-Readable Files (MRFs) mandated under <strong>CMS 45 CFR § 180</strong>. Packaged by health-data initiatives into a modern <strong>DuckLake</strong> columnar Parquet archive containing standard charges, payer-specific negotiated rates, cash discounts, and facility metadata across 7,900+ hospitals.
              </p>
            </div>
            <div className="pt-4 border-t border-slate-100 dark:border-slate-800 mt-4 text-xs text-slate-500 space-y-1">
              <p><strong>Format:</strong> DuckLake 1.0 (Parquet + Metadata)</p>
              <p><strong>Entities:</strong> 7,916 Hospitals, 54 States/Territories</p>
            </div>
          </div>

          {/* Source 2: DOGE Medicare Dataset */}
          <div className="rounded-2xl border border-slate-200 dark:border-slate-800 bg-white dark:bg-slate-900 p-6 flex flex-col justify-between shadow-sm">
            <div className="space-y-3">
              <div className="h-10 w-10 rounded-xl bg-purple-50 dark:bg-purple-950 text-purple-600 dark:text-purple-400 flex items-center justify-center">
                <Cpu className="h-5 w-5" />
              </div>
              <div>
                <span className="text-xs font-semibold text-purple-600 dark:text-purple-400 uppercase tracking-wider block">
                  Government Benchmark
                </span>
                <h3 className="text-base font-bold text-slate-900 dark:text-slate-100 mt-0.5">
                  DOGE / CMS Medicare Dataset
                </h3>
              </div>
              <p className="text-xs text-slate-600 dark:text-slate-400 leading-relaxed">
                Cross-references public Medicare fee-for-service provider payment data from the Department of Government Efficiency (DOGE) and CMS open health datasets. Enables direct comparison of private commercial insurance rates against official Medicare reimbursement baselines.
              </p>
            </div>
            <div className="pt-4 border-t border-slate-100 dark:border-slate-800 mt-4 text-xs text-slate-500 space-y-1">
              <p><strong>Format:</strong> Structured Tabular / Parquet</p>
              <p><strong>Purpose:</strong> Medicare rate baseline & markup ratio</p>
            </div>
          </div>

          {/* Source 3: CMS iQIES POS */}
          <div className="rounded-2xl border border-slate-200 dark:border-slate-800 bg-white dark:bg-slate-900 p-6 flex flex-col justify-between shadow-sm">
            <div className="space-y-3">
              <div className="h-10 w-10 rounded-xl bg-emerald-50 dark:bg-emerald-950 text-emerald-600 dark:text-emerald-400 flex items-center justify-center">
                <FileSpreadsheet className="h-5 w-5" />
              </div>
              <div>
                <span className="text-xs font-semibold text-emerald-600 dark:text-emerald-400 uppercase tracking-wider block">
                  Provider Directory
                </span>
                <h3 className="text-base font-bold text-slate-900 dark:text-slate-100 mt-0.5">
                  CMS iQIES Provider of Services (POS)
                </h3>
              </div>
              <p className="text-xs text-slate-600 dark:text-slate-400 leading-relaxed">
                Official Centers for Medicare & Medicaid Services (CMS) provider directory and data dictionary (<code>iQIES POS Data Dictionary.xlsx</code>). Used to normalize CMS Certification Numbers (CCN), facility street addresses, geographic coordinates, and provider types.
              </p>
            </div>
            <div className="pt-4 border-t border-slate-100 dark:border-slate-800 mt-4 text-xs text-slate-500 space-y-1">
              <p><strong>Authority:</strong> CMS (Department of Health & Human Services)</p>
              <p><strong>Attributes:</strong> CMS ID, NPI, Physical Address, POS Type</p>
            </div>
          </div>
        </div>
      </div>

      {/* Step-by-Step Replication Runbook */}
      <div className="space-y-4">
        <div className="flex items-center justify-between">
          <h2 className="text-lg font-bold text-slate-900 dark:text-slate-100 flex items-center gap-2">
            <Terminal className="h-5 w-5 text-brand-600 dark:text-brand-400" />
            Step-by-Step Local Replication Guide
          </h2>
          <span className="text-xs text-slate-500">Self-contained & fully reproducible</span>
        </div>

        <div className="space-y-4">
          {replicationSteps.map((step, idx) => (
            <div
              key={idx}
              className="rounded-2xl border border-slate-200 dark:border-slate-800 bg-white dark:bg-slate-900 p-5 shadow-sm space-y-2"
            >
              <div className="flex items-center justify-between">
                <h3 className="text-sm font-bold text-slate-900 dark:text-slate-100">
                  {step.title}
                </h3>
                <Button
                  variant="ghost"
                  size="sm"
                  onClick={() => copyToClipboard(step.code, idx)}
                  className="text-xs py-1 px-2 text-slate-500 hover:text-brand-600"
                  icon={copiedIndex === idx ? <Check className="h-3.5 w-3.5 text-emerald-600" /> : <Copy className="h-3.5 w-3.5" />}
                >
                  {copiedIndex === idx ? 'Copied' : 'Copy Commands'}
                </Button>
              </div>
              <p className="text-xs text-slate-600 dark:text-slate-400">
                {step.desc}
              </p>
              <pre className="p-3.5 rounded-xl bg-slate-900 text-slate-100 font-mono text-xs overflow-x-auto selection:bg-brand-500 selection:text-white">
                <code>{step.code}</code>
              </pre>
            </div>
          ))}
        </div>
      </div>

      {/* Lake Data Schema Structure */}
      <div className="p-6 rounded-2xl border border-slate-200 dark:border-slate-800 bg-white dark:bg-slate-900 space-y-4">
        <div className="flex items-center gap-2">
          <BookOpen className="h-5 w-5 text-brand-600 dark:text-brand-400" />
          <h2 className="text-lg font-bold text-slate-900 dark:text-slate-100">
            DuckLake Schema Layout
          </h2>
        </div>
        <p className="text-xs sm:text-sm text-slate-600 dark:text-slate-400">
          When extracted, the <code>lake/</code> folder contains the following hierarchy managed by DuckLake:
        </p>

        <pre className="p-4 rounded-xl bg-slate-900 text-slate-200 font-mono text-xs overflow-x-auto leading-relaxed">
{`lake/
├── open-lake.sh                     # Interactive DuckDB session script
├── open-lake.sql                    # Initialization script with ducklake extension
├── catalog.duckdb                   # Convenience views catalog (current_hospitals, current_charges)
├── metadata.ducklake                # DuckLake snapshot & Parquet registry
└── data/                            # Managed columnar Parquet storage
    └── main/
        ├── hospitals/               # Canonical hospital entities
        ├── hospital_versions/      # Temporal versioning of hospital metadata
        ├── standard_charges/       # Service-level items and procedures
        └── standard_charge_details/# Negotiated rates, cash prices, and insurance plans`}
        </pre>
      </div>
    </div>
  );
};
