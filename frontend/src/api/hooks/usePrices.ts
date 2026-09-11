import { useQuery } from '@tanstack/react-query';
import { api } from '../client';
import type { PriceComparisonParams } from '../types';

export function usePriceComparison(params: PriceComparisonParams) {
  const isEnabled = Boolean(params.code && params.code.trim().length > 0);
  return useQuery({
    queryKey: ['price-comparison', params],
    queryFn: () => api.comparePrices(params),
    enabled: isEnabled,
    staleTime: 60 * 1000,
  });
}
