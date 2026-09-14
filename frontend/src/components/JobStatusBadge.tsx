import type { Job } from '../lib/types'

const STYLES: Record<Job['status'], string> = {
  pending: 'bg-slate-100 text-slate-600',
  running: 'bg-sky-100 text-sky-800',
  completed: 'bg-emerald-100 text-emerald-800',
  failed: 'bg-red-100 text-red-800',
}

export function JobStatusBadge({ job }: { job: Job }) {
  const { progress } = job
  return (
    <div className="flex flex-wrap items-center gap-2 text-xs">
      <span className={`inline-flex items-center rounded-full px-2 py-0.5 font-medium ${STYLES[job.status]}`}>
        {job.stage} — {job.status}
      </span>
      {progress.total > 0 && (
        <span className="text-slate-500">
          {progress.completed + progress.failed} / {progress.total}
          {progress.failed > 0 && <span className="text-red-600"> ({progress.failed} failed)</span>}
        </span>
      )}
      {job.error && <span className="text-red-600">{job.error}</span>}
    </div>
  )
}
