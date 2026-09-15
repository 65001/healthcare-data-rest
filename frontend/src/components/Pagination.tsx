import type { Pagination as PaginationInfo } from '../lib/types'

interface Props {
  pagination: PaginationInfo
  onPageChange: (page: number) => void
}

export function Pagination({ pagination, onPageChange }: Props) {
  const { page, total_pages, total_count, per_page } = pagination
  if (total_count === 0) {
    return <div className="text-sm text-slate-500 dark:text-slate-400">No results.</div>
  }

  const start = (page - 1) * per_page + 1
  const end = Math.min(page * per_page, total_count)

  return (
    <div className="flex items-center justify-between gap-4 text-sm text-slate-600 dark:text-slate-400">
      <span>
        Showing{' '}
        <span className="font-medium text-slate-900 dark:text-slate-100">
          {start}–{end}
        </span>{' '}
        of{' '}
        <span className="font-medium text-slate-900 dark:text-slate-100">
          {total_count.toLocaleString()}
        </span>
      </span>
      <div className="flex items-center gap-2">
        <button
          type="button"
          className="rounded-md border border-slate-300 px-2.5 py-1 hover:bg-slate-100 disabled:cursor-not-allowed disabled:opacity-40 dark:border-slate-700 dark:hover:bg-slate-800"
          disabled={page <= 1}
          onClick={() => onPageChange(page - 1)}
        >
          Prev
        </button>
        <span className="tabular-nums">
          Page {page} / {Math.max(total_pages, 1)}
        </span>
        <button
          type="button"
          className="rounded-md border border-slate-300 px-2.5 py-1 hover:bg-slate-100 disabled:cursor-not-allowed disabled:opacity-40 dark:border-slate-700 dark:hover:bg-slate-800"
          disabled={page >= total_pages}
          onClick={() => onPageChange(page + 1)}
        >
          Next
        </button>
      </div>
    </div>
  )
}
