import { useState } from 'react'
import { Link } from 'react-router-dom'
import { StatCard } from '../components/StatCard'
import { JobStatusBadge } from '../components/JobStatusBadge'
import { useStats } from '../hooks/useStats'
import { useJob, useTriggerEnrich, useTriggerIngest } from '../hooks/usePipeline'

export function DashboardPage() {
  const { data: stats, isLoading, error } = useStats()
  const [retryIncomplete, setRetryIncomplete] = useState(false)

  const ingest = useTriggerIngest()
  const enrich = useTriggerEnrich(retryIncomplete)
  const ingestJob = useJob(ingest.jobId)
  const enrichJob = useJob(enrich.jobId)

  return (
    <div className="flex flex-col gap-8">
      <section>
        <h2 className="mb-4 text-base font-semibold text-slate-900">Overview</h2>
        {isLoading && <p className="text-sm text-slate-500">Loading stats…</p>}
        {error && <p className="text-sm text-red-600">{error.message}</p>}
        {stats && (
          <div className="grid grid-cols-2 gap-4 sm:grid-cols-3 lg:grid-cols-5">
            <StatCard label="Total hospitals" value={stats.total_hospitals.toLocaleString()} />
            <StatCard
              label="Enriched"
              value={stats.enriched.toLocaleString()}
              hint={`${((stats.enriched / Math.max(stats.total_hospitals, 1)) * 100).toFixed(1)}% of total`}
            />
            <StatCard label="MRF found" value={stats.discovery.mrf_found.toLocaleString()} />
            <StatCard label="No website" value={stats.discovery.no_website.toLocaleString()} />
            <StatCard label="Not checked" value={stats.discovery.not_checked.toLocaleString()} />
          </div>
        )}
      </section>

      <section>
        <div className="mb-4 flex items-center justify-between">
          <h2 className="text-base font-semibold text-slate-900">Manual enrichment queue</h2>
          <Link to="/queue" className="text-sm font-medium text-sky-700 hover:underline">
            Open queue →
          </Link>
        </div>
        <p className="text-sm text-slate-600">
          Hospitals the automated pipeline already touched but couldn't fully resolve — still
          missing coordinates, a website, or both — live in the{' '}
          <Link to="/queue" className="text-sky-700 hover:underline">
            Enrichment Queue
          </Link>
          .
        </p>
      </section>

      <section className="rounded-lg border border-slate-200 bg-white p-5">
        <h2 className="mb-1 text-base font-semibold text-slate-900">Pipeline controls</h2>
        <p className="mb-4 text-sm text-slate-500">
          Trigger the ingest/enrich background jobs directly. Both run async on the backend — this
          polls <code className="rounded bg-slate-100 px-1 py-0.5 text-xs">GET /api/pipeline/jobs/:id</code>{' '}
          for progress.
        </p>

        <div className="flex flex-col gap-4 sm:flex-row sm:gap-8">
          <div className="flex flex-col gap-2">
            <button
              type="button"
              onClick={() => ingest.mutate()}
              disabled={ingest.isPending}
              className="rounded-md border border-slate-300 px-3 py-2 text-sm font-medium hover:bg-slate-100 disabled:opacity-40"
            >
              {ingest.isPending ? 'Triggering…' : 'Run CMS ingest'}
            </button>
            {ingestJob.data && <JobStatusBadge job={ingestJob.data} />}
          </div>

          <div className="flex flex-col gap-2">
            <div className="flex items-center gap-3">
              <button
                type="button"
                onClick={() => enrich.mutate()}
                disabled={enrich.isPending}
                className="rounded-md border border-slate-300 px-3 py-2 text-sm font-medium hover:bg-slate-100 disabled:opacity-40"
              >
                {enrich.isPending ? 'Triggering…' : 'Run geocoding enrich'}
              </button>
              <label className="flex items-center gap-1.5 text-xs text-slate-600">
                <input
                  type="checkbox"
                  checked={retryIncomplete}
                  onChange={(e) => setRetryIncomplete(e.target.checked)}
                />
                retry incomplete
              </label>
            </div>
            {enrichJob.data && <JobStatusBadge job={enrichJob.data} />}
          </div>
        </div>
      </section>

      {stats && Object.keys(stats.by_state).length > 0 && (
        <section>
          <h2 className="mb-4 text-base font-semibold text-slate-900">By state</h2>
          <div className="overflow-x-auto rounded-lg border border-slate-200 bg-white">
            <table className="w-full text-sm">
              <thead className="border-b border-slate-200 bg-slate-50 text-left text-xs font-semibold uppercase tracking-wide text-slate-500">
                <tr>
                  <th className="px-4 py-2">State</th>
                  <th className="px-4 py-2">Total</th>
                  <th className="px-4 py-2">MRF found</th>
                  <th className="px-4 py-2">Manifest only</th>
                  <th className="px-4 py-2">No manifest</th>
                  <th className="px-4 py-2">Unreachable</th>
                  <th className="px-4 py-2">No website</th>
                  <th className="px-4 py-2">Not checked</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-slate-100">
                {Object.entries(stats.by_state)
                  .sort((a, b) => a[0].localeCompare(b[0]))
                  .map(([state, breakdown]) => (
                    <tr key={state}>
                      <td className="px-4 py-2 font-medium text-slate-900">{state}</td>
                      <td className="px-4 py-2">{breakdown.total}</td>
                      <td className="px-4 py-2">{breakdown.mrf_found}</td>
                      <td className="px-4 py-2">{breakdown.manifest_only}</td>
                      <td className="px-4 py-2">{breakdown.no_manifest}</td>
                      <td className="px-4 py-2">{breakdown.website_unreachable}</td>
                      <td className="px-4 py-2">{breakdown.no_website}</td>
                      <td className="px-4 py-2">{breakdown.not_checked}</td>
                    </tr>
                  ))}
              </tbody>
            </table>
          </div>
        </section>
      )}
    </div>
  )
}
