import type { MrfMetadata } from '../lib/types'
import { useMrfMetadata, useRecheckMrfMetadata } from '../hooks/useMrfMetadata'
import { JobStatusBadge } from './JobStatusBadge'

const METHOD_LABELS: Record<string, string> = {
  baseline: 'Baseline',
  etag: 'ETag',
  last_modified: 'Last-Modified',
  sha1: 'SHA-1 download',
}

function ChangedPill({ row }: { row: MrfMetadata }) {
  if (row.changed_from_previous === null) {
    return (
      <span className="inline-flex items-center rounded-full bg-slate-100 px-2 py-0.5 text-xs font-medium text-slate-600 dark:bg-slate-800 dark:text-slate-300">
        {row.change_detection_method === 'baseline' ? 'Baseline' : 'Unreachable'}
      </span>
    )
  }
  return row.changed_from_previous ? (
    <span className="inline-flex items-center rounded-full bg-amber-100 px-2 py-0.5 text-xs font-medium text-amber-800 dark:bg-amber-900/40 dark:text-amber-300">
      Changed
    </span>
  ) : (
    <span className="inline-flex items-center rounded-full bg-emerald-100 px-2 py-0.5 text-xs font-medium text-emerald-800 dark:bg-emerald-900/40 dark:text-emerald-300">
      Unchanged
    </span>
  )
}

/** Change-over-time history for one MRF URL, plus a button to re-probe it
 * now. The backend always writes a "baseline" row the moment a discovery
 * is made (see backend/src/routes/pipeline.rs's discover job); this panel
 * is for everything after that — seeing whether the file has changed
 * since, using cache headers when possible and falling back to a full
 * download + SHA-1 hash only when neither ETag nor Last-Modified is
 * usable. */
export function MrfMetadataPanel({ discoveryId }: { discoveryId: string }) {
  const { data: history, isLoading, error } = useMrfMetadata(discoveryId)
  const { mutate: recheck, isPending, job } = useRecheckMrfMetadata(discoveryId)

  const jobInFlight = job && job.status !== 'completed' && job.status !== 'failed'

  return (
    <div className="rounded-lg border border-slate-200 bg-white p-5 dark:border-slate-800 dark:bg-slate-900">
      <div className="mb-3 flex items-center justify-between gap-3">
        <h3 className="text-sm font-semibold text-slate-900 dark:text-slate-100">MRF change history</h3>
        <button
          type="button"
          disabled={isPending || !!jobInFlight}
          onClick={() => recheck()}
          className="rounded-md bg-slate-900 px-3 py-1.5 text-xs font-semibold text-white hover:bg-slate-700 disabled:cursor-not-allowed disabled:opacity-40 dark:bg-slate-100 dark:text-slate-900 dark:hover:bg-slate-300"
        >
          {isPending || jobInFlight ? 'Rechecking…' : 'Recheck now'}
        </button>
      </div>

      {job && <div className="mb-3">{<JobStatusBadge job={job} />}</div>}

      {isLoading && <p className="text-sm text-slate-500 dark:text-slate-400">Loading…</p>}
      {error && <p className="text-sm text-red-600 dark:text-red-400">{error.message}</p>}

      {history && history.length === 0 && (
        <p className="text-sm text-slate-500 dark:text-slate-400">No metadata captured yet.</p>
      )}

      {history && history.length > 0 && (
        <div className="overflow-x-auto">
          <table className="w-full text-left text-sm">
            <thead>
              <tr className="text-xs uppercase tracking-wide text-slate-500 dark:text-slate-400">
                <th className="py-1 pr-3 font-medium">Checked</th>
                <th className="py-1 pr-3 font-medium">Result</th>
                <th className="py-1 pr-3 font-medium">Method</th>
                <th className="py-1 pr-3 font-medium">ETag</th>
                <th className="py-1 pr-3 font-medium">Last-Modified</th>
                <th className="py-1 pr-3 font-medium">Content-Type</th>
                <th className="py-1 pr-3 font-medium">Size</th>
                <th className="py-1 font-medium">SHA-1</th>
              </tr>
            </thead>
            <tbody>
              {history.map((row) => (
                <tr key={row.id} className="border-t border-slate-100 dark:border-slate-800">
                  <td className="py-1.5 pr-3 whitespace-nowrap text-slate-600 dark:text-slate-300">
                    {row.checked_at}
                  </td>
                  <td className="py-1.5 pr-3">
                    <ChangedPill row={row} />
                  </td>
                  <td className="py-1.5 pr-3 text-slate-600 dark:text-slate-300">
                    {row.change_detection_method ? (METHOD_LABELS[row.change_detection_method] ?? row.change_detection_method) : '—'}
                  </td>
                  <td className="max-w-[10rem] truncate py-1.5 pr-3 text-slate-500 dark:text-slate-400" title={row.etag ?? undefined}>
                    {row.etag ?? '—'}
                  </td>
                  <td className="py-1.5 pr-3 whitespace-nowrap text-slate-500 dark:text-slate-400">
                    {row.last_modified ?? '—'}
                  </td>
                  <td className="py-1.5 pr-3 text-slate-500 dark:text-slate-400">{row.content_type ?? '—'}</td>
                  <td className="py-1.5 pr-3 whitespace-nowrap text-slate-500 dark:text-slate-400">
                    {row.content_length != null ? `${row.content_length.toLocaleString()} B` : '—'}
                  </td>
                  <td
                    className="max-w-[8rem] truncate py-1.5 font-mono text-xs text-slate-500 dark:text-slate-400"
                    title={row.sha1_hash ?? undefined}
                  >
                    {row.sha1_hash ?? '—'}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  )
}
