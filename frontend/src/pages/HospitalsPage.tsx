import { useState } from 'react'
import { HospitalsTable } from '../components/HospitalsTable'
import { Pagination } from '../components/Pagination'
import { TableSkeleton } from '../components/Skeleton'
import { useHospitals } from '../hooks/useHospitals'
import { US_STATES } from '../lib/usStates'
import type { ListParams } from '../lib/types'

const ENRICHED_OPTIONS = [
  { label: 'Any', value: '' },
  { label: 'Enriched only', value: 'true' },
  { label: 'Un-enriched only', value: 'false' },
]

export function HospitalsPage() {
  const [state, setState] = useState('')
  const [discoveryStatus, setDiscoveryStatus] = useState('')
  const [enriched, setEnriched] = useState('')
  const [page, setPage] = useState(1)

  const params: ListParams = {
    state: state || undefined,
    discovery_status: discoveryStatus || undefined,
    enriched: enriched === '' ? undefined : enriched === 'true',
    page,
    per_page: 25,
  }
  const { data, isLoading, isFetching, error } = useHospitals(params)

  function updateFilter(fn: () => void) {
    fn()
    setPage(1)
  }

  return (
    <div className="flex flex-col gap-4">
      <div>
        <h2 className="text-base font-semibold text-slate-900 dark:text-slate-100">All hospitals</h2>
        <p className="mt-1 text-sm text-slate-500 dark:text-slate-400">
          Browse the full CMS dataset with its latest MRF discovery status joined in.
        </p>
      </div>

      <div className="flex flex-wrap items-center gap-3 rounded-lg border border-slate-200 bg-white p-3 dark:border-slate-800 dark:bg-slate-900">
        <label className="flex items-center gap-2 text-sm">
          <span className="text-slate-600 dark:text-slate-400">State</span>
          <select
            value={state}
            onChange={(e) => updateFilter(() => setState(e.target.value))}
            className="rounded-md border border-slate-300 px-2 py-1 dark:border-slate-700 dark:bg-slate-800 dark:text-slate-100"
          >
            <option value="">All states</option>
            {US_STATES.map((s) => (
              <option key={s} value={s}>
                {s}
              </option>
            ))}
          </select>
        </label>

        <label className="flex items-center gap-2 text-sm">
          <span className="text-slate-600 dark:text-slate-400">MRF status</span>
          <select
            value={discoveryStatus}
            onChange={(e) => updateFilter(() => setDiscoveryStatus(e.target.value))}
            className="rounded-md border border-slate-300 px-2 py-1 dark:border-slate-700 dark:bg-slate-800 dark:text-slate-100"
          >
            <option value="">Any</option>
            <option value="mrf_found">MRF found</option>
            <option value="manifest_only">Manifest only</option>
            <option value="no_manifest">No manifest</option>
            <option value="website_unreachable">Site unreachable</option>
            <option value="no_website">No website</option>
          </select>
        </label>

        <label className="flex items-center gap-2 text-sm">
          <span className="text-slate-600 dark:text-slate-400">Enrichment</span>
          <select
            value={enriched}
            onChange={(e) => updateFilter(() => setEnriched(e.target.value))}
            className="rounded-md border border-slate-300 px-2 py-1 dark:border-slate-700 dark:bg-slate-800 dark:text-slate-100"
          >
            {ENRICHED_OPTIONS.map((opt) => (
              <option key={opt.value} value={opt.value}>
                {opt.label}
              </option>
            ))}
          </select>
        </label>

        {isFetching && <span className="text-xs text-slate-400 dark:text-slate-500">Refreshing…</span>}
      </div>

      {error && <p className="text-sm text-red-600 dark:text-red-400">{error.message}</p>}

      {isLoading && <TableSkeleton columns={6} />}

      {data && (
        <>
          <HospitalsTable data={data.data} />
          <Pagination pagination={data.pagination} onPageChange={setPage} />
        </>
      )}
    </div>
  )
}
