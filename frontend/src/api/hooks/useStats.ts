import { useQuery } from '@tanstack/react-query';
import { api } from '../client';

export function useDatasetStats() {
  return useQuery({
    queryKey: ['dataset-stats'],
    queryFn: () => api.getStats(),
    staleTime: 5 * 60 * 1000,
    retry: 2,
  });
}
