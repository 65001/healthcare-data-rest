import React, { useMemo } from 'react';
import { formatCurrency } from '../../utils/formatters';
import type { PriceComparisonItem } from '../../api/types';

interface PriceSpreadChartProps {
  items: PriceComparisonItem[];
}

export const PriceSpreadChart: React.FC<PriceSpreadChartProps> = ({ items }) => {
  const data = useMemo(() => {
    const valid = items
      .filter((i): i is PriceComparisonItem & { negotiated_dollar: number } =>
        typeof i.negotiated_dollar === 'number' && i.negotiated_dollar > 0
      )
      .map((i) => ({
        price: i.negotiated_dollar,
        hospital: i.hospital_name,
        payer: i.payer_name || 'Standard',
      }))
      .sort((a, b) => a.price - b.price);

    if (valid.length < 2) return null;

    const min = valid[0].price;
    const max = valid[valid.length - 1].price;
    const range = max - min || 1;

    return {
      valid,
      min,
      max,
      range,
    };
  }, [items]);

  if (!data) return null;

  return (
    <div className="w-full bg-white dark:bg-slate-900 rounded-xl border border-slate-200 dark:border-slate-800 p-5 text-left shadow-sm">
      <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-2 mb-4">
        <div>
          <h3 className="text-sm font-semibold text-slate-900 dark:text-slate-100">
            Price Continuum & Facility Spread
          </h3>
          <p className="text-xs text-slate-500 dark:text-slate-400">
            Visualizing where facilities fall along the low-to-high price spectrum
          </p>
        </div>
        <div className="flex items-center gap-4 text-xs font-mono">
          <span className="text-emerald-600 dark:text-emerald-400 font-semibold">
            Min: {formatCurrency(data.min)}
          </span>
          <span className="text-slate-400">•</span>
          <span className="text-slate-600 dark:text-slate-300 font-semibold">
            Max: {formatCurrency(data.max)}
          </span>
        </div>
      </div>

      {/* Visual spectrum bar */}
      <div className="relative pt-6 pb-2">
        {/* Gradient Track */}
        <div className="h-3 w-full rounded-full bg-gradient-to-r from-emerald-500 via-sky-500 to-rose-500 shadow-inner" />

        {/* Data points */}
        <div className="relative h-4 w-full mt-1">
          {data.valid.map((pt, idx) => {
            const pct = Math.max(0, Math.min(100, ((pt.price - data.min) / data.range) * 100));
            return (
              <div
                key={idx}
                title={`${pt.hospital} (${pt.payer}): ${formatCurrency(pt.price)}`}
                style={{ left: `${pct}%` }}
                className="group absolute -top-4 -translate-x-1/2 cursor-pointer focus:outline-none"
              >
                <div className="h-5 w-1.5 rounded-full bg-slate-900 dark:bg-white shadow border border-white/50 group-hover:h-7 group-hover:w-2 group-hover:bg-brand-500 transition-all" />
              </div>
            );
          })}
        </div>

        {/* Min / Max Labels */}
        <div className="flex justify-between items-center text-[11px] font-medium text-slate-500 dark:text-slate-400 mt-2">
          <span>Most Cost Efficient ({formatCurrency(data.min)})</span>
          <span>Highest Billed ({formatCurrency(data.max)})</span>
        </div>
      </div>
    </div>
  );
};
