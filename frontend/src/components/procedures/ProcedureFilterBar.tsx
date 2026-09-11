import React from 'react';
import { Search, RotateCcw, Sparkles } from 'lucide-react';
import { Button } from '../common/Button';
import { Input } from '../common/Input';
import { Select } from '../common/Select';
import { COMMON_PROCEDURES } from '../../utils/constants';
import type { ProcedureSearchParams } from '../../api/types';

interface ProcedureFilterBarProps {
  params: ProcedureSearchParams;
  onFilterChange: (params: ProcedureSearchParams) => void;
  onSearch: () => void;
  isLoading?: boolean;
}

export const ProcedureFilterBar: React.FC<ProcedureFilterBarProps> = ({
  params,
  onFilterChange,
  onSearch,
  isLoading,
}) => {
  const codeTypeOptions = [
    { value: '', label: 'All Code Types' },
    { value: 'cpt', label: 'CPT® (Outpatient / Office)' },
    { value: 'ms_drg', label: 'MS-DRG (Inpatient)' },
    { value: 'hcpcs', label: 'HCPCS (Supplies & Services)' },
  ];

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    onFilterChange({ ...params, offset: 0 });
    onSearch();
  };

  const handleReset = () => {
    onFilterChange({
      q: '',
      code: '',
      code_type: undefined,
      limit: 20,
      offset: 0,
    });
  };

  return (
    <div className="w-full bg-white dark:bg-slate-900 rounded-2xl border border-slate-200 dark:border-slate-800 shadow-sm p-5 text-left">
      {/* Quick Picks */}
      <div className="mb-4">
        <div className="flex items-center gap-2 mb-2 text-xs font-semibold text-slate-500 dark:text-slate-400">
          <Sparkles className="h-3.5 w-3.5 text-brand-500" />
          <span>Quick Procedure Lookup:</span>
        </div>
        <div className="flex flex-wrap gap-2">
          {COMMON_PROCEDURES.map((preset) => (
            <button
              key={preset.code}
              type="button"
              onClick={() =>
                onFilterChange({
                  ...params,
                  code: preset.code,
                  code_type: preset.codeType,
                  offset: 0,
                })
              }
              className="text-xs px-2.5 py-1 rounded-full bg-slate-100 dark:bg-slate-800 text-slate-700 dark:text-slate-300 hover:bg-slate-200 dark:hover:bg-slate-700 transition-colors font-medium"
            >
              <strong className="font-mono">{preset.code}</strong> — {preset.name}
            </button>
          ))}
        </div>
      </div>

      <form onSubmit={handleSubmit} className="space-y-4">
        <div className="grid grid-cols-1 sm:grid-cols-12 gap-4 items-end">
          <div className="sm:col-span-5">
            <Input
              label="Procedure Description Keyword"
              placeholder="e.g. knee arthroplasty, chest x-ray, MRI"
              value={params.q || ''}
              onChange={(e) => onFilterChange({ ...params, q: e.target.value })}
              icon={<Search className="h-4 w-4" />}
            />
          </div>

          <div className="sm:col-span-4">
            <Input
              label="Specific Code (CPT, HCPCS, DRG)"
              placeholder="e.g. 99213, 470, 71045"
              value={params.code || ''}
              onChange={(e) => onFilterChange({ ...params, code: e.target.value })}
            />
          </div>

          <div className="sm:col-span-3">
            <Select
              label="Code System"
              options={codeTypeOptions}
              value={params.code_type || ''}
              onChange={(e) =>
                onFilterChange({
                  ...params,
                  code_type: (e.target.value as any) || undefined,
                  offset: 0,
                })
              }
            />
          </div>
        </div>

        <div className="flex items-center justify-between pt-2 border-t border-slate-100 dark:border-slate-800">
          <p className="text-xs text-slate-500 dark:text-slate-400">
            Search clinical service codes across all reporting US hospitals.
          </p>

          <div className="flex items-center gap-3">
            <Button
              type="button"
              variant="ghost"
              size="sm"
              onClick={handleReset}
              icon={<RotateCcw className="h-3.5 w-3.5" />}
            >
              Reset
            </Button>
            <Button
              type="submit"
              variant="primary"
              size="md"
              isLoading={isLoading}
              icon={<Search className="h-4 w-4" />}
            >
              Search Procedures
            </Button>
          </div>
        </div>
      </form>
    </div>
  );
};
