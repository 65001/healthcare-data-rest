import { Link, useParams } from 'react-router-dom'
import { useDiscoverHospital, useHospital } from '../hooks/useHospitals'
import { EnrichmentEditor } from '../components/EnrichmentEditor'
import { StatusPill } from '../components/StatusPill'
import { Skeleton } from '../components/Skeleton'
import { JobStatusBadge } from '../components/JobStatusBadge'
import { MrfMetadataPanel } from '../components/MrfMetadataPanel'
import { OwnershipPanel } from '../components/OwnershipPanel'

function Field({ label, value }: { label: string; value: string | number | null | undefined }) {
  return (
    <div>
      <dt className="text-xs font-medium uppercase tracking-wide text-slate-500 dark:text-slate-400">
        {label}
      </dt>
      <dd className="mt-0.5 text-sm text-slate-900 dark:text-slate-100">
        {value ?? <span className="text-slate-400 dark:text-slate-500">—</span>}
      </dd>
    </div>
  )
}

function DetailSkeleton() {
  return (
    <div className="flex flex-col gap-6">
      <div>
        <Skeleton className="h-4 w-24" />
        <Skeleton className="mt-3 h-6 w-64" />
        <Skeleton className="mt-2 h-4 w-20" />
      </div>
      <div className="grid grid-cols-2 gap-4 rounded-lg border border-slate-200 bg-white p-5 sm:grid-cols-3 lg:grid-cols-4 dark:border-slate-800 dark:bg-slate-900">
        {Array.from({ length: 8 }).map((_, i) => (
          <div key={i}>
            <Skeleton className="h-3 w-16" />
            <Skeleton className="mt-1.5 h-4 w-24" />
          </div>
        ))}
      </div>
    </div>
  )
}

export function HospitalDetailPage() {
  const { facilityId } = useParams<{ facilityId: string }>()
  const { data: hospital, isLoading, error } = useHospital(facilityId)
  const discover = useDiscoverHospital(facilityId)
  const discoverInFlight = discover.job && discover.job.status !== 'completed' && discover.job.status !== 'failed'

  if (isLoading) return <DetailSkeleton />
  if (error) return <p className="text-sm text-red-600 dark:text-red-400">{error.message}</p>
  if (!hospital) return null

  return (
    <div className="flex flex-col gap-6">
      <div>
        <Link to="/hospitals" className="text-sm text-sky-700 hover:underline dark:text-sky-400">
          ← All hospitals
        </Link>
        <h2 className="mt-2 text-xl font-semibold text-slate-900 dark:text-slate-100">
          {hospital.facility_name}
        </h2>
        <p className="text-sm text-slate-500 dark:text-slate-400">{hospital.facility_id}</p>
      </div>

      <div className="grid grid-cols-2 gap-4 rounded-lg border border-slate-200 bg-white p-5 sm:grid-cols-3 lg:grid-cols-4 dark:border-slate-800 dark:bg-slate-900">
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

      <div className="rounded-lg border border-slate-200 bg-white p-5 dark:border-slate-800 dark:bg-slate-900">
        <div className="mb-1 flex items-center justify-between gap-3">
          <h3 className="text-sm font-semibold text-slate-900 dark:text-slate-100">Latest MRF discovery</h3>
          <button
            type="button"
            disabled={!hospital.website_url || discover.isPending || !!discoverInFlight}
            onClick={() => discover.mutate()}
            title={hospital.website_url ? undefined : 'This hospital has no website_url on file to probe'}
            className="rounded-md bg-slate-900 px-3 py-1.5 text-xs font-semibold text-white hover:bg-slate-700 disabled:cursor-not-allowed disabled:opacity-40 dark:bg-slate-100 dark:text-slate-900 dark:hover:bg-slate-300"
          >
            {discover.isPending || discoverInFlight ? 'Discovering…' : 'Discover'}
          </button>
        </div>
        {discover.job && <div className="mb-2">{<JobStatusBadge job={discover.job} />}</div>}
        {hospital.latest_discovery ? (
          <>
            <div className="mt-2 flex flex-wrap items-center gap-3 text-sm">
              <StatusPill status={hospital.latest_discovery.discovery_status} />
              <span className="text-slate-500 dark:text-slate-400">
                checked {hospital.latest_discovery.checked_at}
              </span>
              {hospital.latest_discovery.cms_hpt_txt_url && (
                <a
                  href={hospital.latest_discovery.cms_hpt_txt_url}
                  target="_blank"
                  rel="noreferrer"
                  className="text-sky-700 hover:underline dark:text-sky-400"
                >
                  cms-hpt.txt
                </a>
              )}
            </div>
            {(hospital.latest_discovery.contact_name || hospital.latest_discovery.contact_email) && (
              <p className="mt-1.5 text-sm text-slate-500 dark:text-slate-400">
                Manifest contact: {hospital.latest_discovery.contact_name ?? 'Unnamed'}
                {hospital.latest_discovery.contact_email && (
                  <>
                    {' '}
                    &lt;
                    <a
                      href={`mailto:${hospital.latest_discovery.contact_email}`}
                      className="text-sky-700 hover:underline dark:text-sky-400"
                    >
                      {hospital.latest_discovery.contact_email}
                    </a>
                    &gt;
                  </>
                )}
              </p>
            )}
          </>
        ) : (
          <p className="text-sm text-slate-500 dark:text-slate-400">No discovery run yet.</p>
        )}
      </div>

      {hospital.latest_discovery && <MrfMetadataPanel discoveryId={hospital.latest_discovery.id} />}

      <OwnershipPanel facilityId={hospital.facility_id} />

      <div className="rounded-lg border border-slate-200 bg-white p-5 dark:border-slate-800 dark:bg-slate-900">
        <h3 className="mb-3 text-sm font-semibold text-slate-900 dark:text-slate-100">
          Manual enrichment
        </h3>
        <p className="mb-3 text-xs text-slate-500 dark:text-slate-400">
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
