export function formatCurrency(amount: number | null | undefined): string {
  if (amount === null || amount === undefined || isNaN(amount)) {
    return '—';
  }
  return new Intl.NumberFormat('en-US', {
    style: 'currency',
    currency: 'USD',
    minimumFractionDigits: 2,
    maximumFractionDigits: 2,
  }).format(amount);
}

export function formatNumber(num: number | null | undefined): string {
  if (num === null || num === undefined || isNaN(num)) {
    return '—';
  }
  return new Intl.NumberFormat('en-US').format(num);
}

export function formatDate(dateStr: string | null | undefined): string {
  if (!dateStr) return '—';
  try {
    const d = new Date(dateStr);
    if (isNaN(d.getTime())) return dateStr;
    return new Intl.DateTimeFormat('en-US', {
      year: 'numeric',
      month: 'short',
      day: 'numeric',
    }).format(d);
  } catch {
    return dateStr;
  }
}

export function formatCodeType(type: string | null | undefined): string {
  if (!type) return '';
  switch (type.toLowerCase()) {
    case 'cpt':
      return 'CPT®';
    case 'hcpcs':
      return 'HCPCS';
    case 'ms_drg':
    case 'ms-drg':
      return 'MS-DRG';
    default:
      return type.toUpperCase();
  }
}
