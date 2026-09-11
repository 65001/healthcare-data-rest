import React from 'react';
import { twMerge } from 'tailwind-merge';

export interface Option {
  value: string;
  label: string;
}

export interface SelectProps extends React.SelectHTMLAttributes<HTMLSelectElement> {
  label?: string;
  options: Option[];
  placeholder?: string;
  helperText?: string;
  error?: string;
}

export const Select = React.forwardRef<HTMLSelectElement, SelectProps>(
  (
    {
      label,
      options,
      placeholder,
      helperText,
      error,
      id,
      className,
      disabled,
      ...props
    },
    ref
  ) => {
    const selectId = id || React.useId();
    const helperId = `${selectId}-helper`;
    const errorId = `${selectId}-error`;

    return (
      <div className="w-full flex flex-col gap-1.5 text-left">
        {label && (
          <label
            htmlFor={selectId}
            className="text-xs font-semibold text-slate-700 dark:text-slate-200 tracking-wide select-none"
          >
            {label}
          </label>
        )}
        <select
          ref={ref}
          id={selectId}
          disabled={disabled}
          aria-invalid={Boolean(error)}
          aria-describedby={
            error ? errorId : helperText ? helperId : undefined
          }
          className={twMerge(
            'w-full text-sm rounded-lg border bg-white dark:bg-slate-900 text-slate-900 dark:text-slate-100 py-2 px-3 min-h-[40px] transition-colors focus:outline-none focus:ring-2 focus:ring-brand-500 disabled:opacity-50 disabled:bg-slate-50 dark:disabled:bg-slate-800/50',
            error
              ? 'border-rose-500 focus:ring-rose-500'
              : 'border-slate-300 dark:border-slate-700 hover:border-slate-400 dark:hover:border-slate-600',
            className
          )}
          {...props}
        >
          {placeholder && <option value="">{placeholder}</option>}
          {options.map((opt) => (
            <option key={opt.value} value={opt.value}>
              {opt.label}
            </option>
          ))}
        </select>
        {error && (
          <p id={errorId} className="text-xs text-rose-600 dark:text-rose-400 font-medium" role="alert">
            {error}
          </p>
        )}
        {!error && helperText && (
          <p id={helperId} className="text-xs text-slate-500 dark:text-slate-400">
            {helperText}
          </p>
        )}
      </div>
    );
  }
);

Select.displayName = 'Select';
