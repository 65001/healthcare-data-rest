import React from 'react';
import { Search, RotateCcw, MapPin } from 'lucide-react';
import { Button } from '../common/Button';
import { Input } from '../common/Input';
import { Select } from '../common/Select';
import { US_STATES } from '../../utils/constants';
import type { HospitalSearchParams } from '../../api/types';

interface HospitalFilterBarProps {
  params: HospitalSearchParams;
  onFilterChange: (params: HospitalSearchParams) => void;
  onSearch: () => void;
  isLoading?: boolean;
}

export const HospitalFilterBar: React.FC<HospitalFilterBarProps> = ({
  params,
  onFilterChange,
  onSearch,
  isLoading,
}) => {
  const stateOptions = [
    { value: '', label: 'All States & Territories' },
    ...US_STATES.map((s) => ({ value: s.code, label: `${s.code} — ${s.name}` })),
  ];

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    onFilterChange({ ...params, offset: 0 });
    onSearch();
  };

  const handleReset = () => {
    onFilterChange({
      q: '',
      state: '',
      city: '',
      license: '',
      limit: 20,
      offset: 0,
    });
  };

  return (
    <div className="w-full bg-white dark:bg-slate-900 rounded-2xl border border-slate-200 dark:border-slate-800 shadow-sm p-5 text-left">
      <form onSubmit={handleSubmit} className="space-y-4">
        <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-12 gap-4 items-end">
          {/* Main search input */}
          <div className="lg:col-span-5">
            <Input
              label="Hospital Name, Address, or CMS ID"
              placeholder="e.g. Stanford, Mayo, 123 Main St, or 050441"
              value={params.q || ''}
              onChange={(e) => onFilterChange({ ...params, q: e.target.value })}
              icon={<Search className="h-4 w-4" />}
              helperText="Search by facility name, street address, or CMS certification"
            />
          </div>

          {/* State selector */}
          <div className="lg:col-span-3">
            <Select
              label="State / Territory"
              options={stateOptions}
              value={params.state || ''}
              onChange={(e) =>
                onFilterChange({
                  ...params,
                  state: e.target.value || undefined,
                  offset: 0,
                })
              }
            />
          </div>

          {/* City input */}
          <div className="lg:col-span-4">
            <Input
              label="City (Optional)"
              placeholder="e.g. Boston, Dallas, Seattle"
              value={params.city || ''}
              onChange={(e) =>
                onFilterChange({
                  ...params,
                  city: e.target.value || undefined,
                  offset: 0,
                })
              }
              icon={<MapPin className="h-4 w-4" />}
            />
          </div>
        </div>

        {/* Action Row */}
        <div className="flex items-center justify-between pt-2 border-t border-slate-100 dark:border-slate-800">
          <p className="text-xs text-slate-500 dark:text-slate-400">
            Search all hospitals registered in the national DuckLake catalog.
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
              Search Hospitals
            </Button>
          </div>
        </div>
      </form>
    </div>
  );
};
