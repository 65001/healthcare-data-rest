import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { api } from '../lib/api'
import type { ListParams, ManualEnrichmentRequest, NeedsEnrichmentParams } from '../lib/types'

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
