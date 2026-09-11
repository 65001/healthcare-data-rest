import React from 'react';
import { ShieldCheck, Info, Sparkles, ExternalLink } from 'lucide-react';

export const Footer: React.FC = () => {
  return (
    <footer className="w-full border-t border-slate-200 dark:border-slate-800 bg-white dark:bg-slate-900 py-10 mt-auto">
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
        <div className="grid grid-cols-1 md:grid-cols-4 gap-8 mb-8 text-left">
          <div className="md:col-span-2 space-y-3">
            <div className="flex items-center gap-2">
              <span className="font-bold text-base text-slate-900 dark:text-slate-100">
                CarePrice Transparency Engine
              </span>
              <span className="text-[11px] px-2 py-0.5 rounded bg-emerald-100 dark:bg-emerald-950 text-emerald-800 dark:text-emerald-300 font-semibold">
                WCAG 2.1 AA Compliant
              </span>
            </div>
            <p className="text-xs text-slate-500 dark:text-slate-400 leading-relaxed max-w-lg">
              Providing transparent hospital price comparisons across the United States. Powered by official CMS Machine-Readable Files (MRF), DuckDB, DuckLake, and cross-referenced with Medicare datasets.
            </p>
            <div className="flex items-center gap-4 text-xs text-slate-500 dark:text-slate-400 pt-1">
              <span className="inline-flex items-center gap-1">
                <ShieldCheck className="h-3.5 w-3.5 text-emerald-500" />
                CMS Hospital Price Transparency Rule
              </span>
              <span className="inline-flex items-center gap-1">
                <Sparkles className="h-3.5 w-3.5 text-brand-500" />
                AI / MCP Ready
              </span>
            </div>
          </div>

          <div>
            <h2 className="text-xs font-semibold uppercase tracking-wider text-slate-900 dark:text-slate-100 mb-3">
              Standards & Code Systems
            </h2>
            <ul className="space-y-1.5 text-xs text-slate-600 dark:text-slate-400">
              <li>• CPT® (Current Procedural Terminology)</li>
              <li>• HCPCS (Healthcare Common Procedure Coding)</li>
              <li>• MS-DRG (Medicare Severity Inpatient Diagnosis)</li>
              <li>• CMS 10-digit Hospital Identifier</li>
            </ul>
          </div>

          <div>
            <h2 className="text-xs font-semibold uppercase tracking-wider text-slate-900 dark:text-slate-100 mb-3">
              API & Compliance
            </h2>
            <ul className="space-y-1.5 text-xs text-slate-600 dark:text-slate-400">
              <li>
                <a
                  href="/swagger-ui/"
                  target="_blank"
                  rel="noreferrer"
                  className="hover:text-brand-600 dark:hover:text-brand-400 flex items-center gap-1"
                >
                  Interactive Swagger UI
                  <ExternalLink className="h-3 w-3" />
                </a>
              </li>
              <li>
                <a
                  href="/api/v1/openapi.json"
                  target="_blank"
                  rel="noreferrer"
                  className="hover:text-brand-600 dark:hover:text-brand-400 flex items-center gap-1"
                >
                  OpenAPI 3.0 Specification
                  <ExternalLink className="h-3 w-3" />
                </a>
              </li>
              <li>
                <span className="text-slate-400">Section 508 & WCAG 2.1 AA</span>
              </li>
            </ul>
          </div>
        </div>

        <div className="pt-6 border-t border-slate-100 dark:border-slate-800/80 flex flex-col sm:flex-row items-center justify-between gap-4 text-xs text-slate-500 dark:text-slate-400">
          <p>
            © {new Date().getFullYear()} CarePrice. Machine-readable file data is provided for informational and cost-efficiency comparison purposes.
          </p>
          <div className="flex items-center gap-4">
            <span className="flex items-center gap-1">
              <Info className="h-3.5 w-3.5" />
              Not medical advice. Actual out-of-pocket costs depend on deductibles and coinsurance.
            </span>
          </div>
        </div>
      </div>
    </footer>
  );
};
