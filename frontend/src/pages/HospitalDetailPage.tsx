import { Link, useParams } from 'react-router-dom'
import { useHospital } from '../hooks/useHospitals'
import { EnrichmentEditor } from '../components/EnrichmentEditor'
import { StatusPill } from '../components/StatusPill'

function Field({ label, value }: { label: string; value: string | number | null | undefined }) {
  return (
    <div>
      <dt className="text-xs font-medium uppercase tracking-wide text-slate-500">{label}</dt>
      <dd className="mt-0.5 text-sm text-slate-900">{value ?? <span className="text-slate-400">—</span>}</dd>
    </div>
  )
}

export function HospitalDetailPage() {
  const { facilityId } = useParams<{ facilityId: string }>()
  const { data: hospital, isLoading, error } = useHospital(facilityId)

  if (isLoading) return <p className="text-sm text-slate-500">Loading…</p>
  if (error) return <p className="text-sm text-red-600">{error.message}</p>
  if (!hospital) return null

  return (
    <div className="flex flex-col gap-6">
      <div>
        <Link to="/hospitals" className="text-sm text-sky-700 hover:underline">
          ← All hospitals
        </Link>
        <h2 className="mt-2 text-xl font-semibold text-slate-900">{hospital.facility_name}</h2>
        <p className="text-sm text-slate-500">{hospital.facility_id}</p>
      </div>

      <div className="grid grid-cols-2 gap-4 rounded-lg border border-slate-200 bg-white p-5 sm:grid-cols-3 lg:grid-cols-4">
        <Field label="Address" value={hospital.address} />
        <Field label="City" value={hospital.city} />
        <Field label="State" value={hospital.state} />
        <Field label="ZIP" value={hospital.zip_code} />
        <Field label="County" value={hospital.county_name} />
        <Field label="Phone" value={hospital.phone_number} />
        <Field label="Type" value={hospital.hospital_type} />
        <Field label="Ownership" value={hospital.hospital_ownership} />
        <Field label="Emergency services" value={hospital.emergency_services ? 'Yes' : 'No'} />
        <Field label="Overall rating" value={hospital.overall_rating} />
        <Field
          label="Coordinates"
          value={
            hospital.latitude != null && hospital.longitude != null
              ? `${hospital.latitude.toFixed(5)}, ${hospital.longitude.toFixed(5)}`
              : null
          }
        />
        <Field label="Geo provider" value={hospital.geo_provider} />
        <Field label="Website" value={hospital.website_url} />
        <Field label="Enriched at" value={hospital.enriched_at} />
      </div>

      <div className="rounded-lg border border-slate-200 bg-white p-5">
        <h3 className="mb-1 text-sm font-semibold text-slate-900">Latest MRF discovery</h3>
        {hospital.latest_discovery ? (
          <div className="mt-2 flex flex-wrap items-center gap-3 text-sm">
            <StatusPill status={hospital.latest_discovery.discovery_status} />
            <span className="text-slate-500">checked {hospital.latest_discovery.checked_at}</span>
            {hospital.latest_discovery.cms_hpt_txt_url && (
              <a
                href={hospital.latest_discovery.cms_hpt_txt_url}
                target="_blank"
                rel="noreferrer"
                className="text-sky-700 hover:underline"
              >
                cms-hpt.txt
              </a>
            )}
          </div>
        ) : (
          <p className="text-sm text-slate-500">No discovery run yet.</p>
        )}
      </div>

      <div className="rounded-lg border border-slate-200 bg-white p-5">
        <h3 className="mb-3 text-sm font-semibold text-slate-900">Manual enrichment</h3>
        <p className="mb-3 text-xs text-slate-500">
          Overwrites the current coordinates and/or website outright — use this for a correction,
          not just filling a gap.
        </p>
        <EnrichmentEditor
          item={{
            facility_id: hospital.facility_id,
            address: hospital.address,
            city: hospital.city,
            state: hospital.state,
            zip_code: hospital.zip_code,
            missing_coordinates: true,
            missing_website: true,
          }}
        />
      </div>
    </div>
  )
}
