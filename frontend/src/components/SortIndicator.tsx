export function SortIndicator({ direction }: { direction: false | 'asc' | 'desc' }) {
  if (direction === 'asc') return <span className="text-slate-900 dark:text-slate-100">↑</span>
  if (direction === 'desc') return <span className="text-slate-900 dark:text-slate-100">↓</span>
  return <span className="text-slate-400 dark:text-slate-600">⇅</span>
}
