import React from 'react';
import { twMerge } from 'tailwind-merge';

export interface SkeletonProps extends React.HTMLAttributes<HTMLDivElement> {
  className?: string;
}

export const Skeleton: React.FC<SkeletonProps> = ({ className, ...props }) => {
  return (
    <div
      aria-busy="true"
      aria-live="polite"
      className={twMerge(
        'animate-pulse rounded bg-slate-200 dark:bg-slate-800',
        className
      )}
      {...props}
    />
  );
};
