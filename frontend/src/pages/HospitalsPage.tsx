import React, { useState } from 'react';
import { Building2, AlertCircle } from 'lucide-react';
import { HospitalFilterBar } from '../components/hospitals/HospitalFilterBar';
import { HospitalList } from '../components/hospitals/HospitalList';
import { useHospitals } from '../api/hooks/useHospitals';
import type { HospitalSearchParams } from '../api/types';

interface HospitalsPageProps {
  onSelectHospital: (hospitalId: number) => void;
}

export const HospitalsPage: React.FC<HospitalsPageProps> = ({
  onSelectHospital,
}) => {
  const [params, setParams] = useState<HospitalSearchParams>({
    q: '',
    state: '',
    city: '',
    limit: 18,
    offset: 0,
  });

  const { data, isLoading, isError, error, refetch } = useHospitals(params);

  return (
    <div className="space-y-6 max-w-7xl mx-auto w-full text-left">
      {/* Page Header */}
      <div>
        <div className="flex items-center gap-2 mb-1">
          <div className="p-2 rounded-lg bg-brand-50 dark:bg-brand-950 text-brand-600 dark:text-brand-400">
            <Building2 className="h-5 w-5" />
          </div>
          <h1 className="text-2xl sm:text-3xl font-extrabold text-slate-900 dark:text-slate-100 tracking-tight">
            United States Hospital Directory
          </h1>
        </div>
        <p className="text-xs sm:text-sm text-slate-500 dark:text-slate-400">
          Search over 6,000 acute care hospitals, medical centers, and surgical facilities across the 50 states by name, street address, or CMS certification identifier.
        </p>
      </div>

      {/* Filter Bar */}
      <HospitalFilterBar
        params={params}
        onFilterChange={setParams}
        onSearch={() => refetch()}
        isLoading={isLoading}
      />

      {/* Error state */}
      {isError && (
        <div className="p-4 rounded-xl bg-rose-50 dark:bg-rose-950/40 border border-rose-200 dark:border-rose-900 flex items-center gap-3 text-rose-800 dark:text-rose-200 text-sm">
          <AlertCircle className="h-5 w-5 shrink-0 text-rose-600" />
          <span>{error instanceof Error ? error.message : 'Failed to query hospital directory'}</span>
        </div>
      )}

      {/* List / Cards */}
      <HospitalList
        data={data}
        isLoading={isLoading}
        onPageChange={(newOffset) => setParams({ ...params, offset: newOffset })}
        onSelectHospital={onSelectHospital}
      />
    </div>
  );
};
