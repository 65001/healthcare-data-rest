import React, { useMemo } from 'react';
import { ArrowDownRight, TrendingUp, DollarSign, HelpCircle } from 'lucide-react';
import { StatCard } from '../common/StatCard';
import { formatCurrency } from '../../utils/formatters';
import type { PriceComparisonItem } from '../../api/types';

interface PriceStatSummaryProps {
  items: PriceComparisonItem[];
}

export const PriceStatSummary: React.FC<PriceStatSummaryProps> = ({ items }) => {
  const stats = useMemo(() => {
    const validNegotiated = items
      .map((i) => i.negotiated_dollar)
      .filter((p): p is number => typeof p === 'number' && p > 0)
      .sort((a, b) => a - b);

    if (validNegotiated.length === 0) {
      return null;
    }

    const min = validNegotiated[0];
    const max = validNegotiated[validNegotiated.length - 1];
    const midIndex = Math.floor(validNegotiated.length / 2);
    const median =
      validNegotiated.length % 2 !== 0
        ? validNegotiated[midIndex]
        : (validNegotiated[midIndex - 1] + validNegotiated[midIndex]) / 2;

    const lowestItem = items.find((i) => i.negotiated_dollar === min);
    const spreadRatio = min > 0 ? (max / min).toFixed(1) : '—';
    const potentialSavings = max - min;

    return {
      min,
      max,
      median,
      lowestHospitalName: lowestItem?.hospital_name || 'Hospital',
      lowestLocation: [lowestItem?.hospital_city, lowestItem?.hospital_state]
        .filter(Boolean)
        .join(', '),
      spreadRatio,
      potentialSavings,
      totalQuotes: validNegotiated.length,
    };
  }, [items]);

  if (!stats) return null;

  return (
    <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-4 w-full">
      <StatCard
        variant="highlight"
        label="Lowest Rate (Best Value)"
        value={formatCurrency(stats.min)}
        subtext={`At ${stats.lowestHospitalName} ${stats.lowestLocation ? `(${stats.lowestLocation})` : ''}`}
        icon={<ArrowDownRight className="h-5 w-5 text-emerald-600 dark:text-emerald-400" />}
      />

      <StatCard
        variant="accent"
        label="Median Negotiated Rate"
        value={formatCurrency(stats.median)}
        subtext={`Midpoint across ${stats.totalQuotes} rate quotes`}
        icon={<DollarSign className="h-5 w-5 text-brand-600 dark:text-brand-400" />}
      />

      <StatCard
        variant="default"
        label="Highest Rate Found"
        value={formatCurrency(stats.max)}
        subtext="Peak rate for this procedure"
        icon={<TrendingUp className="h-5 w-5 text-slate-500" />}
      />

      <StatCard
        variant="warning"
        label="Price Spread & Potential Savings"
        value={`${stats.spreadRatio}x Variance`}
        subtext={`Up to ${formatCurrency(stats.potentialSavings)} potential price difference`}
        icon={<HelpCircle className="h-5 w-5 text-amber-600 dark:text-amber-400" />}
      />
    </div>
  );
};
