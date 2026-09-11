import { useQuery } from '@tanstack/react-query';
import { api } from '../client';
import type { ProcedureSearchParams } from '../types';

export function useProcedures(params: ProcedureSearchParams) {
  return useQuery({
    queryKey: ['procedures', params],
    queryFn: () => api.getProcedures(params),
    placeholderData: (previousData) => previousData,
    staleTime: 60 * 1000,
  });
}
