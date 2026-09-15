import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { useEffect, useState } from 'react'
import { api } from '../lib/api'
import { useJob } from './usePipeline'

/** Change-over-time history for one mrf_discoveries row, most recent
 * first (oldest entry is always the "baseline" capture made when the
 * discovery itself was found — see backend/src/mrf_metadata.rs). */
export function useMrfMetadata(discoveryId: string | undefined) {
  return useQuery({
    queryKey: ['mrf-metadata', discoveryId],
    queryFn: () => api.getMrfMetadata(discoveryId as string),
    enabled: !!discoveryId,
  })
}

/** Triggers a recheck job for one discovery's MRF URl and, once it
 * completes, invalidates that discovery's metadata history so the newly
 * appended row shows up without a manual refresh. */
export function useRecheckMrfMetadata(discoveryId: string | undefined) {
  const queryClient = useQueryClient()
  const [jobId, setJobId] = useState<string | undefined>(undefined)

  const mutation = useMutation({
    mutationFn: () => api.recheckMrfMetadata(discoveryId as string),
    onSuccess: (job) => setJobId(job.id),
  })

  const job = useJob(jobId)

  useEffect(() => {
    if (job.data?.status === 'completed') {
      queryClient.invalidateQueries({ queryKey: ['mrf-metadata', discoveryId] })
    }
  }, [job.data?.status, discoveryId, queryClient])

  return { ...mutation, job: job.data }
}
