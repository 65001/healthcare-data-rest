import React from 'react';
import { Search, Filter, RotateCcw, Sparkles } from 'lucide-react';
import { Button } from '../common/Button';
import { Input } from '../common/Input';
import { Select } from '../common/Select';
import { COMMON_PROCEDURES, US_STATES } from '../../utils/constants';
import type { PriceComparisonParams } from '../../api/types';

interface CompareFilterBarProps {
  filters: PriceComparisonParams;
  onFilterChange: (filters: PriceComparisonParams) => void;
  onSearch: () => void;
  isLoading?: boolean;
}

export const CompareFilterBar: React.FC<CompareFilterBarProps> = ({
  filters,
  onFilterChange,
  onSearch,
  isLoading,
}) => {
  const handlePresetClick = (preset: typeof COMMON_PROCEDURES[0]) => {
    onFilterChange({
      ...filters,
      code: preset.code,
      code_type: preset.codeType,
    });
  };

  const handleReset = () => {
    onFilterChange({
      code: '99213',
      code_type: 'cpt',
      state: '',
      payer: '',
      plan: '',
      limit: 50,
    });
  };

  const stateOptions = [
    { value: '', label: 'All 50 States & Territories' },
    ...US_STATES.map((s) => ({ value: s.code, label: `${s.code} — ${s.name}` })),
  ];

  const codeTypeOptions = [
    { value: '', label: 'Any Code System' },
    { value: 'cpt', label: 'CPT® (Outpatient & Office)' },
    { value: 'ms_drg', label: 'MS-DRG (Inpatient Hospitalization)' },
    { value: 'hcpcs', label: 'HCPCS (Supplies & Procedures)' },
  ];

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    onSearch();
  };

  return (
    <div className="w-full bg-white dark:bg-slate-900 rounded-2xl border border-slate-200 dark:border-slate-800 shadow-sm p-5 md:p-6 text-left">
      {/* Quick Presets Section */}
      <div className="mb-5">
        <div className="flex items-center gap-2 mb-2 text-xs font-semibold text-slate-500 dark:text-slate-400">
          <Sparkles className="h-3.5 w-3.5 text-brand-500" />
          <span>Popular Procedure Quick-Picks:</span>
        </div>
        <div className="flex flex-wrap gap-2">
          {COMMON_PROCEDURES.map((preset) => {
            const isSelected = filters.code.toLowerCase() === preset.code.toLowerCase();
            return (
              <button
                key={preset.code}
                type="button"
                onClick={() => handlePresetClick(preset)}
                className={`text-xs px-3 py-1.5 rounded-full font-medium transition-all focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-brand-500 ${
                  isSelected
                    ? 'bg-brand-600 text-white shadow-sm ring-2 ring-brand-500 ring-offset-1'
                    : 'bg-slate-100 hover:bg-slate-200 dark:bg-slate-800 dark:hover:bg-slate-700 text-slate-700 dark:text-slate-300'
                }`}
              >
                <span className="font-mono font-bold mr-1">{preset.code}</span>
                <span>{preset.name}</span>
              </button>
            );
          })}
        </div>
      </div>

      {/* Main Filter Inputs Form */}
      <form onSubmit={handleSubmit} className="space-y-4">
        <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-12 gap-4 items-end">
          {/* Procedure Code Input */}
          <div className="lg:col-span-4">
            <Input
              label="Procedure Code (Required)"
              placeholder="e.g. 99213, 470, 70551"
              value={filters.code}
              onChange={(e) =>
                onFilterChange({ ...filters, code: e.target.value })
              }
              required
              icon={<Search className="h-4 w-4" />}
              helperText="Enter CPT, HCPCS, or MS-DRG code"
            />
          </div>

          {/* Code Type */}
          <div className="lg:col-span-2">
            <Select
              label="Code System"
              options={codeTypeOptions}
              value={filters.code_type || ''}
              onChange={(e) =>
                onFilterChange({
                  ...filters,
                  code_type: (e.target.value as any) || undefined,
                })
              }
            />
          </div>

          {/* State Filter */}
          <div className="lg:col-span-3">
            <Select
              label="Geographic Region (State)"
              options={stateOptions}
              value={filters.state || ''}
              onChange={(e) =>
                onFilterChange({
                  ...filters,
                  state: e.target.value || undefined,
                })
              }
            />
          </div>

          {/* Insurance Payer Filter */}
          <div className="lg:col-span-3">
            <Input
              label="Insurance Payer (Optional)"
              placeholder="e.g. Aetna, Blue Cross, Cigna"
              value={filters.payer || ''}
              onChange={(e) =>
                onFilterChange({
                  ...filters,
                  payer: e.target.value || undefined,
                })
              }
              helperText="Filter by insurer name"
            />
          </div>
        </div>

        {/* Action Buttons Row */}
        <div className="flex flex-wrap items-center justify-between gap-3 pt-2 border-t border-slate-100 dark:border-slate-800">
          <div className="flex items-center gap-2 text-xs text-slate-500 dark:text-slate-400">
            <Filter className="h-3.5 w-3.5" />
            <span>Comparing negotiated insurance rates and discounted cash prices.</span>
          </div>

          <div className="flex items-center gap-3">
            <Button
              type="button"
              variant="ghost"
              size="sm"
              onClick={handleReset}
              icon={<RotateCcw className="h-3.5 w-3.5" />}
            >
              Reset Filters
            </Button>
            <Button
              type="submit"
              variant="primary"
              size="md"
              isLoading={isLoading}
              icon={<Search className="h-4 w-4" />}
            >
              Compare Prices
            </Button>
          </div>
        </div>
      </form>
    </div>
  );
};
