import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { useState } from 'react'
import { api } from '../lib/api'
import type { DiscoverRequest } from '../lib/types'

/** Polls GET /api/pipeline/jobs/:id every second while the job is
 * pending/running; stops once it lands on completed/failed. Jobs are
 * in-memory only on the backend (see backend/src/jobs.rs), so this is
 * only meaningful for the lifetime of one backend process. */
export function useJob(jobId: string | undefined) {
  return useQuery({
    queryKey: ['job', jobId],
    queryFn: () => api.getJob(jobId as string),
    enabled: !!jobId,
    refetchInterval: (query) => {
      const status = query.state.data?.status
      return status === 'completed' || status === 'failed' ? false : 1000
    },
  })
}

/** Tracks the most recently triggered job of a given kind (ingest/enrich)
 * so the UI can show its live status without a separate store. */
export function usePipelineTrigger(trigger: () => Promise<{ id: string }>) {
  const queryClient = useQueryClient()
  const [jobId, setJobId] = useState<string | undefined>(undefined)

  const mutation = useMutation({
    mutationFn: trigger,
    onSuccess: (job) => {
      setJobId(job.id)
      // The hospitals/stats views become stale once the job finishes, but
      // there's no push channel — an invalidate on trigger + one on job
      // completion (handled by callers via the job's status) is the best
      // available approximation.
      queryClient.invalidateQueries({ queryKey: ['stats'] })
    },
  })

  return { ...mutation, jobId }
}

export function useTriggerIngest() {
  return usePipelineTrigger(api.triggerIngest)
}

export function useTriggerEnrich(retryIncomplete: boolean) {
  return usePipelineTrigger(() => api.triggerEnrich(retryIncomplete))
}

export function useTriggerIngestOwnership() {
  return usePipelineTrigger(api.triggerIngestOwnership)
}

/** Runs POST /api/pipeline/discover with the given body. An unset
 * `network_manifest_url` selects the per-hospital-website mode — pass
 * `{}` to probe every hospital with a website on file, or narrow with
 * `state`/`facility_ids`. For one specific hospital, prefer
 * `useDiscoverHospital` in useHospitals.ts instead, which also refreshes
 * that hospital's detail view on completion. */
export function useTriggerDiscover(body: DiscoverRequest) {
  return usePipelineTrigger(() => api.triggerDiscover(body))
}
