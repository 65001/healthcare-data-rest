import React, { useState } from 'react';
import { Scale, Sparkles, AlertCircle } from 'lucide-react';
import { CompareFilterBar } from '../components/compare/CompareFilterBar';
import { PriceStatSummary } from '../components/compare/PriceStatSummary';
import { PriceSpreadChart } from '../components/compare/PriceSpreadChart';
import { PriceComparisonTable } from '../components/compare/PriceComparisonTable';
import { PriceCardMobile } from '../components/compare/PriceCardMobile';
import { Skeleton } from '../components/common/Skeleton';
import { usePriceComparison } from '../api/hooks/usePrices';
import type { PriceComparisonParams } from '../api/types';

interface ComparePageProps {
  initialCode?: string;
  onSelectHospital: (hospitalId: number) => void;
}

export const ComparePage: React.FC<ComparePageProps> = ({
  initialCode = '99213',
  onSelectHospital,
}) => {
  const [filters, setFilters] = useState<PriceComparisonParams>({
    code: initialCode,
    code_type: 'cpt',
    state: '',
    payer: '',
    plan: '',
    limit: 50,
  });

  const { data: comparisonItems, isLoading, isError, error, refetch } =
    usePriceComparison(filters);

  const minPrice = React.useMemo(() => {
    if (!comparisonItems) return null;
    const valid = comparisonItems
      .map((i) => i.negotiated_dollar)
      .filter((p): p is number => typeof p === 'number' && p > 0);
    return valid.length > 0 ? Math.min(...valid) : null;
  }, [comparisonItems]);

  return (
    <div className="space-y-6 max-w-7xl mx-auto w-full text-left">
      {/* Hero Banner */}
      <div className="bg-gradient-to-br from-brand-900 via-brand-800 to-slate-900 text-white rounded-3xl p-6 sm:p-10 shadow-xl relative overflow-hidden">
        <div className="absolute right-0 top-0 translate-x-10 -translate-y-10 w-96 h-96 bg-brand-500/10 rounded-full blur-3xl pointer-events-none" />
        <div className="max-w-2xl relative z-10">
          <div className="inline-flex items-center gap-2 px-3 py-1 rounded-full bg-white/10 backdrop-blur text-xs font-semibold text-brand-200 mb-4 border border-white/10">
            <Sparkles className="h-3.5 w-3.5 text-sky-400" />
            Healthcare Price Transparency & Cost Efficiency
          </div>
          <h1 className="text-3xl sm:text-4xl lg:text-5xl font-extrabold tracking-tight leading-tight text-white mb-3">
            Find the Most Cost-Effective Hospital for Your Care
          </h1>
          <p className="text-slate-300 text-sm sm:text-base leading-relaxed">
            Compare negotiated insurance rates, cash discounts, and chargemaster gross rates across facilities in your region. Save hundreds or thousands on planned procedures.
          </p>
        </div>
      </div>

      {/* Filter Bar */}
      <CompareFilterBar
        filters={filters}
        onFilterChange={setFilters}
        onSearch={() => refetch()}
        isLoading={isLoading}
      />

      {/* Error state */}
      {isError && (
        <div className="p-5 rounded-2xl bg-rose-50 dark:bg-rose-950/40 border border-rose-200 dark:border-rose-900 flex items-start gap-3 text-rose-900 dark:text-rose-200 text-sm">
          <AlertCircle className="h-5 w-5 shrink-0 text-rose-600 mt-0.5" />
          <div>
            <span className="font-semibold block">Failed to load price comparison data</span>
            <span className="text-xs text-rose-700 dark:text-rose-300">
              {error instanceof Error ? error.message : 'Unknown database error'}
            </span>
          </div>
        </div>
      )}

      {/* Loading state */}
      {isLoading && (
        <div className="space-y-4">
          <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-4">
            {Array.from({ length: 4 }).map((_, i) => (
              <Skeleton key={i} className="h-28 rounded-xl" />
            ))}
          </div>
          <Skeleton className="h-44 rounded-xl" />
          <Skeleton className="h-96 rounded-2xl" />
        </div>
      )}

      {/* Results view */}
      {!isLoading && comparisonItems && comparisonItems.length > 0 && (
        <div className="space-y-6">
          {/* Key Metrics */}
          <PriceStatSummary items={comparisonItems} />

          {/* Visual Distribution */}
          <PriceSpreadChart items={comparisonItems} />

          {/* Desktop Table View */}
          <div className="hidden md:block">
            <PriceComparisonTable
              items={comparisonItems}
              onSelectHospital={onSelectHospital}
            />
          </div>

          {/* Mobile Cards View */}
          <div className="md:hidden space-y-3">
            <div className="flex items-center justify-between text-xs text-slate-500 px-1">
              <span>{comparisonItems.length} Comparison Quotes</span>
              <span>Sorted by lowest price</span>
            </div>
            {comparisonItems.map((item, idx) => (
              <PriceCardMobile
                key={idx}
                item={item}
                isBestValue={
                  item.negotiated_dollar !== null &&
                  item.negotiated_dollar !== undefined &&
                  item.negotiated_dollar === minPrice
                }
                onSelectHospital={onSelectHospital}
              />
            ))}
          </div>
        </div>
      )}

      {/* Empty State when zero quotes returned */}
      {!isLoading && comparisonItems && comparisonItems.length === 0 && (
        <div className="bg-white dark:bg-slate-900 rounded-2xl border border-slate-200 dark:border-slate-800 p-12 text-center">
          <Scale className="h-12 w-12 text-slate-400 mx-auto mb-3" />
          <h3 className="text-base font-semibold text-slate-900 dark:text-slate-100">
            No Pricing Quotes for Code "{filters.code}"
          </h3>
          <p className="text-xs text-slate-500 dark:text-slate-400 max-w-md mx-auto mt-1">
            No hospital machine-readable files currently reported pricing for this specific code under the chosen state/payer filter. Try picking a popular preset above or clearing geographic filters.
          </p>
        </div>
      )}
    </div>
  );
};
