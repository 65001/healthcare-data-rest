import React from 'react';
import {
  ChevronLeft,
  ChevronRight,
  Search,
  Scale,
  Building2,
} from 'lucide-react';
import { Badge } from '../common/Badge';
import { Button } from '../common/Button';
import { Skeleton } from '../common/Skeleton';
import { formatCurrency, formatNumber } from '../../utils/formatters';
import type { PaginatedResponse, StandardCharge } from '../../api/types';

interface ProcedureTableProps {
  data?: PaginatedResponse<StandardCharge>;
  isLoading: boolean;
  onPageChange: (newOffset: number) => void;
  onCompare: (code: string) => void;
  onSelectHospital: (hospitalId: number) => void;
}

export const ProcedureTable: React.FC<ProcedureTableProps> = ({
  data,
  isLoading,
  onPageChange,
  onCompare,
  onSelectHospital,
}) => {
  if (isLoading) {
    return (
      <div className="bg-white dark:bg-slate-900 rounded-2xl border border-slate-200 dark:border-slate-800 p-6 space-y-4">
        {Array.from({ length: 6 }).map((_, i) => (
          <Skeleton key={i} className="h-12 w-full rounded-lg" />
        ))}
      </div>
    );
  }

  if (!data || data.items.length === 0) {
    return (
      <div className="bg-white dark:bg-slate-900 rounded-2xl border border-slate-200 dark:border-slate-800 p-12 text-center">
        <Search className="h-12 w-12 text-slate-400 mx-auto mb-3" />
        <h3 className="text-base font-semibold text-slate-900 dark:text-slate-100">
          No Procedures Found
        </h3>
        <p className="text-xs text-slate-500 dark:text-slate-400 max-w-sm mx-auto mt-1">
          Try searching for a standard code (e.g. 99213, 470) or clinical keywords like "joint" or "scan".
        </p>
      </div>
    );
  }

  const { items, limit, offset } = data;
  const currentPage = Math.floor(offset / limit) + 1;
  const totalPages = data.mode === 'exact' ? (Math.ceil(data.total / limit) || 1) : null;
  const hasPrev = offset > 0;
  const hasNext = data.mode === 'exact' ? offset + limit < data.total : data.has_more;

  return (
    <div className="w-full bg-white dark:bg-slate-900 rounded-2xl border border-slate-200 dark:border-slate-800 shadow-sm overflow-hidden text-left">
      <div className="p-4 border-b border-slate-100 dark:border-slate-800 flex items-center justify-between text-xs text-slate-500 dark:text-slate-400">
        <span>
          {data.mode === 'exact' ? (
            <>
              Showing <strong className="text-slate-900 dark:text-slate-100">{offset + 1}–{Math.min(offset + limit, data.total)}</strong> of{' '}
              <strong className="text-slate-900 dark:text-slate-100">{formatNumber(data.total)}</strong> procedure entries
            </>
          ) : (
            <>
              Showing <strong className="text-slate-900 dark:text-slate-100">{offset + 1}–{offset + items.length}</strong> procedure entries
            </>
          )}
        </span>
        <span>{totalPages ? `Page ${currentPage} of ${totalPages}` : `Page ${currentPage}`}</span>
      </div>

      <div className="overflow-x-auto">
        <table className="w-full text-left text-sm border-collapse" aria-label="Procedure Charges Directory Table">
          <thead className="bg-slate-50 dark:bg-slate-800/60 text-xs font-semibold text-slate-600 dark:text-slate-300 border-b border-slate-200 dark:border-slate-700">
            <tr>
              <th className="py-3 px-4">Code</th>
              <th className="py-3 px-4">Description</th>
              <th className="py-3 px-4">Facility</th>
              <th className="py-3 px-4 text-right">Gross Charge</th>
              <th className="py-3 px-4 text-right">Discounted Cash</th>
              <th className="py-3 px-4 text-right">Avg Negotiated</th>
              <th className="py-3 px-4 text-right">Actions</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-slate-100 dark:divide-slate-800">
            {items.map((item, idx) => {
              const code = item.cpt || item.ms_drg || item.hcpcs || '';
              const codeType = item.cpt ? 'CPT' : item.ms_drg ? 'MS-DRG' : item.hcpcs ? 'HCPCS' : '';

              return (
                <tr key={`${item.charge_id}-${item.charge_seq ?? 0}-${idx}`} className="hover:bg-slate-50/70 dark:hover:bg-slate-800/40 transition-colors">
                  <td className="py-3 px-4 whitespace-nowrap">
                    {code ? (
                      <div className="flex items-center gap-1.5 font-mono">
                        <Badge variant="brand" size="sm">
                          {code}
                        </Badge>
                        <span className="text-[10px] text-slate-400 font-semibold">{codeType}</span>
                      </div>
                    ) : (
                      <span className="text-slate-400 text-xs">—</span>
                    )}
                  </td>
                  <td className="py-3 px-4 max-w-sm">
                    <p className="font-medium text-slate-900 dark:text-slate-100 line-clamp-2 text-xs sm:text-sm">
                      {item.description}
                    </p>
                    {item.setting && (
                      <span className="text-[11px] text-slate-500 capitalize">
                        Setting: {item.setting}
                      </span>
                    )}
                  </td>
                  <td className="py-3 px-4">
                    <button
                      type="button"
                      onClick={() => onSelectHospital(item.hospital_id)}
                      className="text-xs font-semibold text-brand-600 dark:text-brand-400 hover:underline flex items-center gap-1 text-left"
                    >
                      <Building2 className="h-3 w-3 shrink-0" />
                      <span>{item.hospital_name || `Hospital #${item.hospital_id}`}</span>
                    </button>
                  </td>
                  <td className="py-3 px-4 text-right font-mono text-xs text-slate-400">
                    {formatCurrency(item.gross_charge)}
                  </td>
                  <td className="py-3 px-4 text-right font-mono text-xs font-semibold text-slate-800 dark:text-slate-200">
                    {formatCurrency(item.discounted_cash)}
                  </td>
                  <td className="py-3 px-4 text-right font-mono text-xs font-bold text-emerald-600 dark:text-emerald-400">
                    {formatCurrency(item.avg_negotiated_rate)}
                  </td>
                  <td className="py-3 px-4 text-right whitespace-nowrap">
                    {code && (
                      <Button
                        variant="primary"
                        size="sm"
                        onClick={() => onCompare(code)}
                        className="text-xs py-1"
                        icon={<Scale className="h-3.5 w-3.5" />}
                      >
                        Compare
                      </Button>
                    )}
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>

      <div className="flex items-center justify-between border-t border-slate-200 dark:border-slate-800 p-4">
        <Button
          variant="outline"
          size="sm"
          disabled={!hasPrev}
          onClick={() => onPageChange(Math.max(0, offset - limit))}
          icon={<ChevronLeft className="h-4 w-4" />}
        >
          Previous
        </Button>

        <span className="text-xs font-medium text-slate-600 dark:text-slate-400">
          {totalPages ? `Page ${currentPage} of ${totalPages}` : `Page ${currentPage}`}
        </span>

        <Button
          variant="outline"
          size="sm"
          disabled={!hasNext}
          onClick={() => onPageChange(offset + limit)}
        >
          <span>Next</span>
          <ChevronRight className="h-4 w-4 ml-1" />
        </Button>
      </div>
    </div>
  );
};
