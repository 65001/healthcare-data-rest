import React, { useState } from 'react';
import { Search, AlertCircle } from 'lucide-react';
import { ProcedureFilterBar } from '../components/procedures/ProcedureFilterBar';
import { ProcedureTable } from '../components/procedures/ProcedureTable';
import { useProcedures } from '../api/hooks/useProcedures';
import type { ProcedureSearchParams } from '../api/types';

interface ProceduresPageProps {
  onCompare: (code: string) => void;
  onSelectHospital: (hospitalId: number) => void;
}

export const ProceduresPage: React.FC<ProceduresPageProps> = ({
  onCompare,
  onSelectHospital,
}) => {
  const [params, setParams] = useState<ProcedureSearchParams>({
    q: '',
    code: '',
    code_type: undefined,
    limit: 20,
    offset: 0,
  });

  const { data, isLoading, isError, error, refetch } = useProcedures(params);

  return (
    <div className="space-y-6 max-w-7xl mx-auto w-full text-left">
      {/* Page Header */}
      <div>
        <div className="flex items-center gap-2 mb-1">
          <div className="p-2 rounded-lg bg-brand-50 dark:bg-brand-950 text-brand-600 dark:text-brand-400">
            <Search className="h-5 w-5" />
          </div>
          <h1 className="text-2xl sm:text-3xl font-extrabold text-slate-900 dark:text-slate-100 tracking-tight">
            Procedure & Standard Charges Directory
          </h1>
        </div>
        <p className="text-xs sm:text-sm text-slate-500 dark:text-slate-400">
          Explore standard chargemaster entries, negotiated average rates, and discounted cash prices for procedures across the United States.
        </p>
      </div>

      {/* Filter Bar */}
      <ProcedureFilterBar
        params={params}
        onFilterChange={setParams}
        onSearch={() => refetch()}
        isLoading={isLoading}
      />

      {/* Error state */}
      {isError && (
        <div className="p-4 rounded-xl bg-rose-50 dark:bg-rose-950/40 border border-rose-200 dark:border-rose-900 flex items-center gap-3 text-rose-800 dark:text-rose-200 text-sm">
          <AlertCircle className="h-5 w-5 shrink-0 text-rose-600" />
          <span>{error instanceof Error ? error.message : 'Failed to query procedures'}</span>
        </div>
      )}

      {/* Table */}
      <ProcedureTable
        data={data}
        isLoading={isLoading}
        onPageChange={(newOffset) => setParams({ ...params, offset: newOffset })}
        onCompare={onCompare}
        onSelectHospital={onSelectHospital}
      />
    </div>
  );
};
