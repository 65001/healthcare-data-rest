import React from 'react';
import { twMerge } from 'tailwind-merge';

export interface InputProps extends React.InputHTMLAttributes<HTMLInputElement> {
  label?: string;
  helperText?: string;
  error?: string;
  icon?: React.ReactNode;
  rightElement?: React.ReactNode;
}

export const Input = React.forwardRef<HTMLInputElement, InputProps>(
  (
    {
      label,
      helperText,
      error,
      icon,
      rightElement,
      id,
      className,
      disabled,
      ...props
    },
    ref
  ) => {
    const inputId = id || React.useId();
    const helperId = `${inputId}-helper`;
    const errorId = `${inputId}-error`;

    return (
      <div className="w-full flex flex-col gap-1.5 text-left">
        {label && (
          <label
            htmlFor={inputId}
            className="text-xs font-semibold text-slate-700 dark:text-slate-200 tracking-wide select-none"
          >
            {label}
          </label>
        )}
        <div className="relative flex items-center">
          {icon && (
            <div
              className="absolute left-3 flex items-center pointer-events-none text-slate-400 dark:text-slate-500"
              aria-hidden="true"
            >
              {icon}
            </div>
          )}
          <input
            ref={ref}
            id={inputId}
            disabled={disabled}
            aria-invalid={Boolean(error)}
            aria-describedby={
              error ? errorId : helperText ? helperId : undefined
            }
            className={twMerge(
              'w-full text-sm rounded-lg border bg-white dark:bg-slate-900 text-slate-900 dark:text-slate-100 placeholder:text-slate-400 dark:placeholder:text-slate-500 transition-colors focus:outline-none focus:ring-2 focus:ring-brand-500 disabled:opacity-50 disabled:bg-slate-50 dark:disabled:bg-slate-800/50',
              icon ? 'pl-9' : 'pl-3.5',
              rightElement ? 'pr-10' : 'pr-3.5',
              'py-2 min-h-[40px]',
              error
                ? 'border-rose-500 focus:ring-rose-500'
                : 'border-slate-300 dark:border-slate-700 hover:border-slate-400 dark:hover:border-slate-600',
              className
            )}
            {...props}
          />
          {rightElement && (
            <div className="absolute right-3 flex items-center">
              {rightElement}
            </div>
          )}
        </div>
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

Input.displayName = 'Input';
