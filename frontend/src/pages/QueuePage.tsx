import { useState } from 'react'
import { EnrichmentQueueTable } from '../components/EnrichmentQueueTable'
import { Pagination } from '../components/Pagination'
import { useNeedsEnrichment } from '../hooks/useHospitals'
import { US_STATES } from '../lib/usStates'
import type { NeedsEnrichmentParams } from '../lib/types'

export function QueuePage() {
  const [missing, setMissing] = useState<NeedsEnrichmentParams['missing']>(undefined)
  const [state, setState] = useState('')
  const [page, setPage] = useState(1)

  const params: NeedsEnrichmentParams = {
    missing,
    state: state || undefined,
    page,
    per_page: 25,
  }
  const { data, isLoading, isFetching, error } = useNeedsEnrichment(params)

  function updateFilter(fn: () => void) {
    fn()
    setPage(1)
  }

  return (
    <div className="flex flex-col gap-4">
      <div>
        <h2 className="text-base font-semibold text-slate-900">Manual enrichment queue</h2>
        <p className="mt-1 text-sm text-slate-500">
          Hospitals the automated pipeline already ran on but couldn't fully resolve. Fill in
          coordinates and/or a website by hand — saving stamps <code className="rounded bg-slate-100 px-1 py-0.5 text-xs">geo_provider = "manual"</code>.
        </p>
      </div>

      <div className="flex flex-wrap items-center gap-3 rounded-lg border border-slate-200 bg-white p-3">
        <label className="flex items-center gap-2 text-sm">
          <span className="text-slate-600">Missing</span>
          <select
            value={missing ?? ''}
            onChange={(e) =>
              updateFilter(() =>
                setMissing((e.target.value || undefined) as NeedsEnrichmentParams['missing']),
              )
            }
            className="rounded-md border border-slate-300 px-2 py-1"
          >
            <option value="">Either</option>
            <option value="coordinates">Coordinates</option>
            <option value="website">Website</option>
          </select>
        </label>

        <label className="flex items-center gap-2 text-sm">
          <span className="text-slate-600">State</span>
          <select
            value={state}
            onChange={(e) => updateFilter(() => setState(e.target.value))}
            className="rounded-md border border-slate-300 px-2 py-1"
          >
            <option value="">All states</option>
            {US_STATES.map((s) => (
              <option key={s} value={s}>
                {s}
              </option>
            ))}
          </select>
        </label>

        {isFetching && <span className="text-xs text-slate-400">Refreshing…</span>}
      </div>

      {isLoading && <p className="text-sm text-slate-500">Loading queue…</p>}
      {error && <p className="text-sm text-red-600">{error.message}</p>}

      {data && (
        <>
          <EnrichmentQueueTable data={data.data} />
          <Pagination pagination={data.pagination} onPageChange={setPage} />
        </>
      )}
    </div>
  )
}
