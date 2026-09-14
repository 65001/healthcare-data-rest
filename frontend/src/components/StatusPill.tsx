const STYLES: Record<string, string> = {
  mrf_found: 'bg-emerald-100 text-emerald-800',
  manifest_only: 'bg-amber-100 text-amber-800',
  no_manifest: 'bg-orange-100 text-orange-800',
  website_unreachable: 'bg-red-100 text-red-800',
  no_website: 'bg-slate-200 text-slate-700',
  not_checked: 'bg-slate-100 text-slate-500',
}

const LABELS: Record<string, string> = {
  mrf_found: 'MRF found',
  manifest_only: 'Manifest only',
  no_manifest: 'No manifest',
  website_unreachable: 'Site unreachable',
  no_website: 'No website',
  not_checked: 'Not checked',
}

export function StatusPill({ status }: { status: string | null | undefined }) {
  const key = status ?? 'not_checked'
  const style = STYLES[key] ?? STYLES.not_checked
  const label = LABELS[key] ?? key
  return (
    <span className={`inline-flex items-center rounded-full px-2 py-0.5 text-xs font-medium ${style}`}>
      {label}
    </span>
  )
}
