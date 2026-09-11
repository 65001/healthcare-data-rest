import React from 'react';
import {
  ShieldCheck,
  Database,
  Cpu,
  Bot,
  ExternalLink,
  Code2,
} from 'lucide-react';
import { Badge } from '../components/common/Badge';
import { useDatasetStats } from '../api/hooks/useStats';
import { formatNumber } from '../utils/formatters';

export const AboutPage: React.FC = () => {
  const { data: stats } = useDatasetStats();

  return (
    <div className="space-y-8 max-w-5xl mx-auto w-full text-left">
      {/* Intro */}
      <div>
        <div className="flex items-center gap-2 mb-2">
          <Badge variant="brand" size="md">
            Architecture & Transparency
          </Badge>
          <Badge variant="success" size="md">
            WCAG 2.1 AA Compliant
          </Badge>
        </div>
        <h1 className="text-3xl sm:text-4xl font-extrabold text-slate-900 dark:text-slate-100 tracking-tight">
          About CarePrice & Healthcare Transparency
        </h1>
        <p className="mt-2 text-sm sm:text-base text-slate-600 dark:text-slate-400 leading-relaxed">
          CarePrice is designed to demystify hospital costs across the United States. We ingest machine-readable files (MRF) published under federal transparency mandates, normalize charges across thousands of facilities, and present clean, cost-comparative insights for consumers, employers, and AI agents.
        </p>
      </div>

      {/* Dataset Statistics Card */}
      {stats && (
        <div className="p-6 rounded-2xl bg-gradient-to-r from-brand-900 to-slate-900 text-white shadow-md">
          <h2 className="text-sm font-semibold uppercase tracking-wider text-brand-300 mb-4">
            Live DuckLake Telemetry
          </h2>
          <div className="grid grid-cols-2 sm:grid-cols-4 gap-4">
            <div>
              <span className="text-xs text-slate-400 block">Total Hospitals</span>
              <span className="text-2xl font-bold font-mono text-white">
                {formatNumber(stats.total_hospitals)}
              </span>
            </div>
            <div>
              <span className="text-xs text-slate-400 block">States & Territories</span>
              <span className="text-2xl font-bold font-mono text-white">
                {stats.states_covered}
              </span>
            </div>
            <div>
              <span className="text-xs text-slate-400 block">Lake Engine</span>
              <span className="text-2xl font-bold font-mono text-sky-400">
                DuckDB & DuckLake
              </span>
            </div>
            <div>
              <span className="text-xs text-slate-400 block">Attachment Status</span>
              <span className="text-base font-bold text-emerald-400 flex items-center gap-1.5 mt-1">
                <span className="h-2.5 w-2.5 rounded-full bg-emerald-400 animate-ping" />
                {stats.is_lake_attached ? 'Online & Queryable' : 'Offline'}
              </span>
            </div>
          </div>
        </div>
      )}

      {/* Feature Grid */}
      <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
        {/* Card 1: Mandate */}
        <div className="p-6 rounded-2xl border border-slate-200 dark:border-slate-800 bg-white dark:bg-slate-900 space-y-3">
          <div className="h-10 w-10 rounded-xl bg-blue-50 dark:bg-blue-950 text-blue-600 dark:text-blue-400 flex items-center justify-center">
            <ShieldCheck className="h-5 w-5" />
          </div>
          <h2 className="text-lg font-bold text-slate-900 dark:text-slate-100">
            CMS Price Transparency Mandate
          </h2>
          <p className="text-xs sm:text-sm text-slate-600 dark:text-slate-400 leading-relaxed">
            Under 45 CFR § 180, hospitals operating in the US are legally required to establish, update, and make public a list of their standard charges for items and services, including gross charges, discounted cash prices, and payer-specific negotiated rates.
          </p>
        </div>

        {/* Card 2: DuckLake */}
        <div className="p-6 rounded-2xl border border-slate-200 dark:border-slate-800 bg-white dark:bg-slate-900 space-y-3">
          <div className="h-10 w-10 rounded-xl bg-amber-50 dark:bg-amber-950 text-amber-600 dark:text-amber-400 flex items-center justify-center">
            <Database className="h-5 w-5" />
          </div>
          <h2 className="text-lg font-bold text-slate-900 dark:text-slate-100">
            DuckDB & DuckLake Storage
          </h2>
          <p className="text-xs sm:text-sm text-slate-600 dark:text-slate-400 leading-relaxed">
            Our high-concurrency backend is written in Rust (Tokio & Axum) directly connected to a DuckLake analytical data lake, scanning millions of standard charges in milliseconds using columnar Parquet files.
          </p>
        </div>

        {/* Card 3: AI & MCP */}
        <div className="p-6 rounded-2xl border border-slate-200 dark:border-slate-800 bg-white dark:bg-slate-900 space-y-3">
          <div className="h-10 w-10 rounded-xl bg-emerald-50 dark:bg-emerald-950 text-emerald-600 dark:text-emerald-400 flex items-center justify-center">
            <Bot className="h-5 w-5" />
          </div>
          <h2 className="text-lg font-bold text-slate-900 dark:text-slate-100">
            AI-First & Model Context Protocol (MCP)
          </h2>
          <p className="text-xs sm:text-sm text-slate-600 dark:text-slate-400 leading-relaxed">
            Designed to be queried not only by humans but also by AI agents. An associated Model Context Protocol (MCP) server enables Claude, ChatGPT, and autonomous assistants to execute price comparison queries for patients.
          </p>
        </div>

        {/* Card 4: DOGE & Medicare */}
        <div className="p-6 rounded-2xl border border-slate-200 dark:border-slate-800 bg-white dark:bg-slate-900 space-y-3">
          <div className="h-10 w-10 rounded-xl bg-purple-50 dark:bg-purple-950 text-purple-600 dark:text-purple-400 flex items-center justify-center">
            <Cpu className="h-5 w-5" />
          </div>
          <h2 className="text-lg font-bold text-slate-900 dark:text-slate-100">
            Medicare & DOGE Cross-Referencing
          </h2>
          <p className="text-xs sm:text-sm text-slate-600 dark:text-slate-400 leading-relaxed">
            Commercial insurer rates are benchmarked against Medicare fee-for-service base reimbursement and DOGE dataset payer cross-references, clarifying hospital markup multiples above actual government rates.
          </p>
        </div>
      </div>

      {/* Developer / API Section */}
      <div className="p-6 rounded-2xl border border-slate-200 dark:border-slate-800 bg-white dark:bg-slate-900 space-y-4">
        <div className="flex items-center gap-2">
          <Code2 className="h-5 w-5 text-brand-600 dark:text-brand-400" />
          <h2 className="text-lg font-bold text-slate-900 dark:text-slate-100">
            Developer API Access
          </h2>
        </div>
        <p className="text-xs sm:text-sm text-slate-600 dark:text-slate-400">
          The CarePrice REST API exposes comprehensive endpoints with OpenAPI 3.0 schemas and interactive Swagger UI documentation.
        </p>

        <div className="bg-slate-900 text-slate-100 rounded-xl p-4 font-mono text-xs overflow-x-auto space-y-2">
          <p className="text-slate-400"># Sample curl command to compare CPT 99213 prices:</p>
          <p className="text-emerald-400">
            curl -s "http://localhost:3000/api/v1/prices/compare?code=99213&state=CA" | jq .
          </p>
        </div>

        <div className="flex flex-wrap gap-4 pt-2">
          <a
            href="/swagger-ui/"
            target="_blank"
            rel="noreferrer"
            className="inline-flex items-center gap-1.5 text-xs font-semibold text-brand-600 dark:text-brand-400 hover:underline"
          >
            Launch Interactive Swagger UI
            <ExternalLink className="h-3.5 w-3.5" />
          </a>
          <a
            href="/api/v1/openapi.json"
            target="_blank"
            rel="noreferrer"
            className="inline-flex items-center gap-1.5 text-xs font-semibold text-brand-600 dark:text-brand-400 hover:underline"
          >
            Download OpenAPI Specification (JSON)
            <ExternalLink className="h-3.5 w-3.5" />
          </a>
        </div>
      </div>
    </div>
  );
};
