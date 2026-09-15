import { useState } from 'react'
import { usePatchEnrichment } from '../hooks/useHospitals'

/** Whatever the editor needs to identify a hospital, build a "find on map"
 * link, and decide which fields to show. Satisfied by both
 * `NeedsEnrichmentItem` (queue rows, which set the `missing_*` flags) and
 * a hand-built object from the hospital detail page (which shows both
 * fields regardless, since a manual correction there is opt-in either way). */
export interface EnrichmentTarget {
  facility_id: string
  address: string
  city: string
  state: string
  zip_code: string
  missing_coordinates: boolean
  missing_website: boolean
}

interface Props {
  item: EnrichmentTarget
}

function isValidLat(v: string) {
  if (v.trim() === '') return true
  const n = Number(v)
  return Number.isFinite(n) && n >= -90 && n <= 90
}
function isValidLon(v: string) {
  if (v.trim() === '') return true
  const n = Number(v)
  return Number.isFinite(n) && n >= -180 && n <= 180
}
function isValidUrl(v: string) {
  if (v.trim() === '') return true
  return v.startsWith('http://') || v.startsWith('https://')
}

/** Inline editor for one row of GET /api/hospitals/needs-enrichment.
 * Mirrors the validation backend/src/routes/hospitals.rs::patch_enrichment
 * enforces, so bad input is caught before the round trip. Lat/lon must
 * both be filled or both left blank; website is independent. */
export function EnrichmentEditor({ item }: Props) {
  const [lat, setLat] = useState('')
  const [lon, setLon] = useState('')
  const [website, setWebsite] = useState('')
  const { mutate, isPending, isSuccess, error, reset } = usePatchEnrichment()

  const latOk = isValidLat(lat)
  const lonOk = isValidLon(lon)
  const urlOk = isValidUrl(website)
  const coordsPaired = (lat.trim() === '') === (lon.trim() === '')
  const hasAnyInput = lat.trim() !== '' || lon.trim() !== '' || website.trim() !== ''
  const canSave = hasAnyInput && latOk && lonOk && urlOk && coordsPaired

  const mapsQuery = encodeURIComponent(
    `${item.address}, ${item.city}, ${item.state} ${item.zip_code}`,
  )

  function handleSave() {
    if (!canSave) return
    reset()
    mutate({
      facilityId: item.facility_id,
      body: {
        latitude: lat.trim() === '' ? undefined : Number(lat),
        longitude: lon.trim() === '' ? undefined : Number(lon),
        website_url: website.trim() === '' ? undefined : website.trim(),
      },
    })
  }

  return (
    <div className="flex flex-col gap-2">
      {item.missing_coordinates && (
        <div className="flex items-center gap-2">
          <input
            type="text"
            inputMode="decimal"
            placeholder="Latitude"
            value={lat}
            onChange={(e) => setLat(e.target.value)}
            className={`w-28 rounded-md border px-2 py-1 text-sm dark:bg-slate-900 dark:text-slate-100 ${
              latOk ? 'border-slate-300 dark:border-slate-700' : 'border-red-400 bg-red-50 dark:bg-red-950/40'
            }`}
          />
          <input
            type="text"
            inputMode="decimal"
            placeholder="Longitude"
            value={lon}
            onChange={(e) => setLon(e.target.value)}
            className={`w-28 rounded-md border px-2 py-1 text-sm dark:bg-slate-900 dark:text-slate-100 ${
              lonOk ? 'border-slate-300 dark:border-slate-700' : 'border-red-400 bg-red-50 dark:bg-red-950/40'
            }`}
          />
          <a
            href={`https://www.google.com/maps/search/?api=1&query=${mapsQuery}`}
            target="_blank"
            rel="noreferrer"
            className="whitespace-nowrap text-xs font-medium text-sky-700 hover:underline dark:text-sky-400"
          >
            Find on map ↗
          </a>
        </div>
      )}
      {item.missing_website && (
        <input
          type="text"
          placeholder="https://hospital-website.org"
          value={website}
          onChange={(e) => setWebsite(e.target.value)}
          className={`w-full rounded-md border px-2 py-1 text-sm dark:bg-slate-900 dark:text-slate-100 ${
            urlOk ? 'border-slate-300 dark:border-slate-700' : 'border-red-400 bg-red-50 dark:bg-red-950/40'
          }`}
        />
      )}

      {!coordsPaired && (
        <p className="text-xs text-red-600 dark:text-red-400">
          Provide both latitude and longitude, or neither.
        </p>
      )}
      {error && <p className="text-xs text-red-600 dark:text-red-400">{error.message}</p>}
      {isSuccess && <p className="text-xs text-emerald-600 dark:text-emerald-400">Saved.</p>}

      <div>
        <button
          type="button"
          disabled={!canSave || isPending}
          onClick={handleSave}
          className="rounded-md bg-slate-900 px-3 py-1.5 text-xs font-semibold text-white hover:bg-slate-700 disabled:cursor-not-allowed disabled:opacity-40 dark:bg-slate-100 dark:text-slate-900 dark:hover:bg-slate-300"
        >
          {isPending ? 'Saving…' : 'Save'}
        </button>
      </div>
    </div>
  )
}
