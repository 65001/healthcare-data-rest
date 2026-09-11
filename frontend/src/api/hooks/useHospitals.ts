import { useQuery } from '@tanstack/react-query';
import { api } from '../client';
import type { HospitalSearchParams } from '../types';

export function useHospitals(params: HospitalSearchParams) {
  return useQuery({
    queryKey: ['hospitals', params],
    queryFn: () => api.getHospitals(params),
    placeholderData: (previousData) => previousData,
    staleTime: 60 * 1000,
  });
}

export function useHospital(id: number | null | undefined) {
  return useQuery({
    queryKey: ['hospital', id],
    queryFn: () => (id ? api.getHospitalById(id) : Promise.reject('No ID provided')),
    enabled: typeof id === 'number' && !isNaN(id),
    staleTime: 5 * 60 * 1000,
  });
}
