export function Skeleton({ className = '' }: { className?: string }) {
  return (
    <div
      className={`animate-pulse rounded-md bg-slate-200 dark:bg-slate-800 ${className}`}
      aria-hidden="true"
    />
  )
}

export function StatCardSkeleton() {
  return (
    <div className="rounded-lg border border-slate-200 bg-white p-4 dark:border-slate-800 dark:bg-slate-900">
      <Skeleton className="h-3 w-20" />
      <Skeleton className="mt-2 h-7 w-16" />
    </div>
  )
}

/** Placeholder rows shown only on a cold load (no cached data yet) — see
 * each page's `isLoading` check. A background refetch keeps the previous
 * page's real rows via TanStack Query's `placeholderData`, so this never
 * flashes on pagination/filter changes, only on first mount. */
export function TableSkeleton({ columns, rows = 8 }: { columns: number; rows?: number }) {
  return (
    <div className="overflow-hidden rounded-lg border border-slate-200 bg-white dark:border-slate-800 dark:bg-slate-900">
      <table className="w-full text-sm">
        <tbody className="divide-y divide-slate-100 dark:divide-slate-800">
          {Array.from({ length: rows }).map((_, rowIdx) => (
            <tr key={rowIdx}>
              {Array.from({ length: columns }).map((_, colIdx) => (
                <td key={colIdx} className="px-4 py-3">
                  <Skeleton className={`h-4 ${colIdx === 0 ? 'w-32' : 'w-20'}`} />
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}
