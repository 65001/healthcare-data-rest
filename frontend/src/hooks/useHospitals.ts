import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { useEffect, useState } from 'react'
import { api } from '../lib/api'
import type { ListParams, ManualEnrichmentRequest, NeedsEnrichmentParams } from '../lib/types'
import { useJob } from './usePipeline'

export function useHospitals(params: ListParams) {
  return useQuery({
    queryKey: ['hospitals', params],
    queryFn: () => api.listHospitals(params),
    placeholderData: (prev) => prev,
  })
}

export function useHospital(facilityId: string | undefined) {
  return useQuery({
    queryKey: ['hospital', facilityId],
    queryFn: () => api.getHospital(facilityId as string),
    enabled: !!facilityId,
  })
}

export function useHospitalOwnership(facilityId: string | undefined) {
  return useQuery({
    queryKey: ['hospital-ownership', facilityId],
    queryFn: () => api.getHospitalOwnership(facilityId as string),
    enabled: !!facilityId,
  })
}

/** Runs MRF discovery for exactly one hospital (POST /api/pipeline/discover
 * with `facility_ids: [facilityId]`, the per-hospital-website mode since
 * no `network_manifest_url` is given) and refreshes that hospital's
 * detail view once the job completes, so a newly found `latest_discovery`
 * shows up without a manual reload. */
export function useDiscoverHospital(facilityId: string | undefined) {
  const queryClient = useQueryClient()
  const [jobId, setJobId] = useState<string | undefined>(undefined)

  const mutation = useMutation({
    mutationFn: () => api.triggerDiscover({ facility_ids: facilityId ? [facilityId] : [] }),
    onSuccess: (job) => setJobId(job.id),
  })

  const job = useJob(jobId)

  useEffect(() => {
    if (job.data?.status === 'completed') {
      queryClient.invalidateQueries({ queryKey: ['hospital', facilityId] })
      queryClient.invalidateQueries({ queryKey: ['stats'] })
    }
  }, [job.data?.status, facilityId, queryClient])

  return { ...mutation, job: job.data }
}

export function useNeedsEnrichment(params: NeedsEnrichmentParams) {
  return useQuery({
    queryKey: ['needs-enrichment', params],
    queryFn: () => api.needsEnrichment(params),
    placeholderData: (prev) => prev,
  })
}

/** PATCH one hospital's manual enrichment fields, refreshing every list that
 * could show it (the full list, the needs-enrichment queue, and stats). */
export function usePatchEnrichment() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: ({ facilityId, body }: { facilityId: string; body: ManualEnrichmentRequest }) =>
      api.patchEnrichment(facilityId, body),
    onSuccess: (_data, { facilityId }) => {
      queryClient.invalidateQueries({ queryKey: ['hospitals'] })
      queryClient.invalidateQueries({ queryKey: ['needs-enrichment'] })
      queryClient.invalidateQueries({ queryKey: ['hospital', facilityId] })
      queryClient.invalidateQueries({ queryKey: ['stats'] })
    },
  })
}
