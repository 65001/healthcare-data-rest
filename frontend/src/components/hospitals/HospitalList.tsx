import React from 'react';
import { ChevronLeft, ChevronRight, Building2 } from 'lucide-react';
import { HospitalCard } from './HospitalCard';
import { Skeleton } from '../common/Skeleton';
import { Button } from '../common/Button';
import { formatNumber } from '../../utils/formatters';
import type { HospitalSummary, PaginatedResponse } from '../../api/types';

interface HospitalListProps {
  data?: PaginatedResponse<HospitalSummary>;
  isLoading: boolean;
  onPageChange: (newOffset: number) => void;
  onSelectHospital: (hospitalId: number) => void;
}

export const HospitalList: React.FC<HospitalListProps> = ({
  data,
  isLoading,
  onPageChange,
  onSelectHospital,
}) => {
  if (isLoading) {
    return (
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4 w-full">
        {Array.from({ length: 6 }).map((_, idx) => (
          <div
            key={idx}
            className="p-5 rounded-xl border border-slate-200 dark:border-slate-800 bg-white dark:bg-slate-900 space-y-3"
          >
            <div className="flex justify-between items-center">
              <Skeleton className="h-10 w-10 rounded-lg" />
              <Skeleton className="h-5 w-24 rounded-full" />
            </div>
            <Skeleton className="h-5 w-3/4" />
            <Skeleton className="h-4 w-1/2" />
            <div className="pt-3 border-t border-slate-100 dark:border-slate-800 flex justify-between">
              <Skeleton className="h-4 w-20" />
              <Skeleton className="h-8 w-24" />
            </div>
          </div>
        ))}
      </div>
    );
  }

  if (!data || data.items.length === 0) {
    return (
      <div className="w-full bg-white dark:bg-slate-900 rounded-2xl border border-slate-200 dark:border-slate-800 p-12 text-center">
        <Building2 className="h-12 w-12 text-slate-400 mx-auto mb-3" />
        <h3 className="text-base font-semibold text-slate-900 dark:text-slate-100">
          No Hospitals Found
        </h3>
        <p className="text-xs text-slate-500 dark:text-slate-400 max-w-sm mx-auto mt-1">
          Try expanding your search query, removing state filters, or checking for typo variations.
        </p>
      </div>
    );
  }

  const { items, limit, offset } = data;
  const total = data.mode === 'exact' ? data.total : items.length;
  const currentPage = Math.floor(offset / limit) + 1;
  const totalPages = Math.ceil(total / limit) || 1;
  const hasPrev = offset > 0;
  const hasNext = data.mode === 'exact' ? offset + limit < data.total : data.has_more;

  return (
    <div className="w-full space-y-6 text-left">
      {/* Total Found Count */}
      <div className="flex items-center justify-between text-xs text-slate-500 dark:text-slate-400 px-1">
        <span>
          Showing <strong className="text-slate-800 dark:text-slate-200">{offset + 1}–{Math.min(offset + limit, total)}</strong> of{' '}
          <strong className="text-slate-800 dark:text-slate-200">{formatNumber(total)}</strong> hospitals
        </span>
        <span>
          Page {currentPage} of {totalPages}
        </span>
      </div>

      {/* Hospital Cards Grid */}
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
        {items.map((hospital, idx) => (
          <HospitalCard
            key={`${hospital.hospital_id}-${hospital.hospital_name}-${idx}`}
            hospital={hospital}
            onSelect={onSelectHospital}
          />
        ))}
      </div>

      {/* Accessible Pagination Navigation */}
      <div className="flex items-center justify-between border-t border-slate-200 dark:border-slate-800 pt-4 px-1">
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
          Page {currentPage} of {totalPages}
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
