import React from 'react';
import { CheckCircle2, MapPin, ExternalLink } from 'lucide-react';
import { Badge } from '../common/Badge';
import { Button } from '../common/Button';
import { formatCurrency } from '../../utils/formatters';
import type { PriceComparisonItem } from '../../api/types';

interface PriceCardMobileProps {
  item: PriceComparisonItem;
  isBestValue?: boolean;
  onSelectHospital: (hospitalId: number) => void;
}

export const PriceCardMobile: React.FC<PriceCardMobileProps> = ({
  item,
  isBestValue,
  onSelectHospital,
}) => {
  return (
    <div
      className={`rounded-xl border p-4 text-left shadow-sm transition-all ${
        isBestValue
          ? 'border-emerald-300 dark:border-emerald-800 bg-emerald-50/40 dark:bg-emerald-950/20'
          : 'border-slate-200 dark:border-slate-800 bg-white dark:bg-slate-900'
      }`}
    >
      <div className="flex items-start justify-between gap-2 mb-2">
        <div>
          <div className="flex items-center gap-1.5 flex-wrap">
            <h4 className="font-semibold text-slate-900 dark:text-slate-100 text-sm">
              {item.hospital_name}
            </h4>
            {isBestValue && (
              <Badge variant="success" size="sm">
                <CheckCircle2 className="h-3 w-3" />
                Best Value
              </Badge>
            )}
          </div>
          <p className="flex items-center gap-1 text-xs text-slate-500 dark:text-slate-400 mt-0.5">
            <MapPin className="h-3 w-3" />
            {[item.hospital_city, item.hospital_state].filter(Boolean).join(', ') || 'USA'}
          </p>
        </div>
      </div>

      <div className="grid grid-cols-2 gap-2 my-3 p-2.5 rounded-lg bg-slate-50 dark:bg-slate-800/60 text-xs">
        <div>
          <span className="text-slate-500 dark:text-slate-400 block text-[11px]">Negotiated Rate</span>
          <span className="font-bold text-base font-mono text-slate-900 dark:text-slate-100">
            {formatCurrency(item.negotiated_dollar)}
          </span>
        </div>
        <div>
          <span className="text-slate-500 dark:text-slate-400 block text-[11px]">Discounted Cash</span>
          <span className="font-semibold font-mono text-slate-700 dark:text-slate-300">
            {formatCurrency(item.discounted_cash)}
          </span>
        </div>
        <div>
          <span className="text-slate-500 dark:text-slate-400 block text-[11px]">Gross (List) Charge</span>
          <span className="font-mono text-slate-400 line-through">
            {formatCurrency(item.gross_charge)}
          </span>
        </div>
        <div>
          <span className="text-slate-500 dark:text-slate-400 block text-[11px]">Payer / Plan</span>
          <span className="font-medium truncate block text-slate-700 dark:text-slate-200">
            {item.payer_name || 'Standard'}
          </span>
        </div>
      </div>

      <div className="flex items-center justify-between pt-1">
        {item.setting && (
          <Badge variant="neutral" size="sm">
            {item.setting}
          </Badge>
        )}
        <Button
          variant="outline"
          size="sm"
          onClick={() => onSelectHospital(item.hospital_id)}
          icon={<ExternalLink className="h-3 w-3" />}
        >
          View Hospital
        </Button>
      </div>
    </div>
  );
};
