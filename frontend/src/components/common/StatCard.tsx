import React from 'react';
import { twMerge } from 'tailwind-merge';

export interface StatCardProps {
  label: string;
  value: string | number;
  subtext?: string;
  icon?: React.ReactNode;
  variant?: 'default' | 'highlight' | 'accent' | 'warning';
  className?: string;
}

export const StatCard: React.FC<StatCardProps> = ({
  label,
  value,
  subtext,
  icon,
  variant = 'default',
  className,
}) => {
  const variantStyles = {
    default:
      'bg-white dark:bg-slate-900 border-slate-200 dark:border-slate-800 text-slate-900 dark:text-slate-100',
    highlight:
      'bg-emerald-50/70 dark:bg-emerald-950/30 border-emerald-200 dark:border-emerald-800 text-emerald-950 dark:text-emerald-100',
    accent:
      'bg-brand-50/70 dark:bg-brand-950/30 border-brand-200 dark:border-brand-800 text-brand-950 dark:text-brand-100',
    warning:
      'bg-amber-50/70 dark:bg-amber-950/30 border-amber-200 dark:border-amber-800 text-amber-950 dark:text-amber-100',
  };

  return (
    <div
      className={twMerge(
        'rounded-xl border p-4 shadow-sm transition-all duration-200 flex flex-col justify-between text-left',
        variantStyles[variant],
        className
      )}
    >
      <div className="flex items-center justify-between gap-2">
        <span className="text-xs font-semibold uppercase tracking-wider text-slate-500 dark:text-slate-400">
          {label}
        </span>
        {icon && (
          <div className="text-slate-400 dark:text-slate-500" aria-hidden="true">
            {icon}
          </div>
        )}
      </div>
      <div className="mt-2">
        <div className="text-2xl font-bold tracking-tight">
          {value}
        </div>
        {subtext && (
          <p className="mt-1 text-xs text-slate-500 dark:text-slate-400 font-medium">
            {subtext}
          </p>
        )}
      </div>
    </div>
  );
};
