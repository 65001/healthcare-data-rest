import { Link } from 'react-router-dom'
import type { HospitalOwner } from '../lib/types'
import { useHospitalOwnership } from '../hooks/useHospitals'

const TYPE_LABELS: Record<string, string> = {
  O: 'Organization',
  I: 'Individual',
}

function ownerName(owner: HospitalOwner): string {
  return owner.owner_organization_name ?? owner.owner_person_name ?? '—'
}

function formatPercentage(value: string | null): string {
  if (value === null || value.trim() === '') return '—'
  return /%$/.test(value.trim()) ? value.trim() : `${value.trim()}%`
}

/** CMS's PECOS "ROLE TEXT - OWNER" values mix two different things under
 * one table: an actual financial ownership/partnership/mortgage-debt
 * stake, versus a management or control role (corporate officer/director,
 * W-2 managing employee, operational/managerial control) that carries no
 * equity — real rows confirm this: every management-role row this session
 * has seen has `percentage_ownership: null`. Ownership-interest roles are
 * matched by keyword since their exact wording varies (direct/indirect,
 * X%, mortgage debt, partnership); everything else is a management role. */
function isOwnershipInterestRole(owner: HospitalOwner): boolean {
  const role = (owner.owner_role_text ?? '').toUpperCase()
  if (/OWNERSHIP|PARTNERSHIP|MORTGAGE/.test(role)) return true
  if (role !== '') return false
  // No role text at all — fall back to whether a stake was actually disclosed.
  return owner.percentage_ownership !== null && owner.percentage_ownership.trim() !== ''
}

interface GroupedManager {
  key: string
  name: string
  type: string | null
  roles: string[]
}

/** Groups management/control rows by owner (name + type) into a dict
 * keyed on that identity, accumulating every distinct role text seen for
 * them — real data has the same person listed once per role (e.g. Dennis
 * Matheis: CORPORATE OFFICER, CORPORATE DIRECTOR, and W-2 MANAGING
 * EMPLOYEE as three separate rows), so this collapses that back into one
 * row per person with their roles rendered as chips. */
function groupByOwner(owners: HospitalOwner[]): GroupedManager[] {
  const byOwner = new Map<string, GroupedManager>()
  for (const owner of owners) {
    const name = ownerName(owner)
    const key = `${owner.owner_type ?? ''}:${name}`
    const role = owner.owner_role_text ?? '—'

    const existing = byOwner.get(key)
    if (existing) {
      if (!existing.roles.includes(role)) existing.roles.push(role)
    } else {
      byOwner.set(key, { key, name, type: owner.owner_type, roles: [role] })
    }
  }
  return Array.from(byOwner.values())
}

function RoleChip({ role }: { role: string }) {
  return (
    <span className="inline-flex items-center rounded-full bg-slate-100 px-2 py-0.5 text-xs font-medium text-slate-700 dark:bg-slate-800 dark:text-slate-300">
      {role}
    </span>
  )
}

function ManagementTable({ owners }: { owners: HospitalOwner[] }) {
  const grouped = groupByOwner(owners)
  return (
    <div className="overflow-x-auto">
      <table className="w-full text-left text-sm">
        <thead>
          <tr className="text-xs uppercase tracking-wide text-slate-500 dark:text-slate-400">
            <th className="py-1 pr-3 font-medium">Owner</th>
            <th className="py-1 pr-3 font-medium">Type</th>
            <th className="py-1 font-medium">Roles</th>
          </tr>
        </thead>
        <tbody>
          {grouped.map((manager) => (
            <tr key={manager.key} className="border-t border-slate-100 dark:border-slate-800">
              <td className="py-1.5 pr-3 align-top font-medium text-slate-900 dark:text-slate-100">{manager.name}</td>
              <td className="py-1.5 pr-3 align-top text-slate-600 dark:text-slate-300">
                {manager.type ? (TYPE_LABELS[manager.type] ?? manager.type) : '—'}
              </td>
              <td className="py-1.5">
                <div className="flex flex-wrap gap-1">
                  {manager.roles.map((role) => (
                    <RoleChip key={role} role={role} />
                  ))}
                </div>
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}

function OwnerTable({ owners, showPercentage }: { owners: HospitalOwner[]; showPercentage: boolean }) {
  return (
    <div className="overflow-x-auto">
      <table className="w-full text-left text-sm">
        <thead>
          <tr className="text-xs uppercase tracking-wide text-slate-500 dark:text-slate-400">
            <th className="py-1 pr-3 font-medium">Owner</th>
            <th className="py-1 pr-3 font-medium">Type</th>
            <th className="py-1 pr-3 font-medium">Role</th>
            {showPercentage && <th className="py-1 font-medium">Ownership</th>}
          </tr>
        </thead>
        <tbody>
          {owners.map((owner, i) => (
            <tr key={`${owner.enrollment_id}-${i}`} className="border-t border-slate-100 dark:border-slate-800">
              <td className="py-1.5 pr-3 font-medium text-slate-900 dark:text-slate-100">{ownerName(owner)}</td>
              <td className="py-1.5 pr-3 text-slate-600 dark:text-slate-300">
                {owner.owner_type ? (TYPE_LABELS[owner.owner_type] ?? owner.owner_type) : '—'}
              </td>
              <td className="py-1.5 pr-3 text-slate-500 dark:text-slate-400">{owner.owner_role_text ?? '—'}</td>
              {showPercentage && (
                <td className="py-1.5 text-slate-500 dark:text-slate-400">{formatPercentage(owner.percentage_ownership)}</td>
              )}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}

/** Disclosed owners/controllers from CMS's PECOS ownership data (see
 * GET /api/hospitals/:id/ownership) — the same data
 * POST /api/pipeline/discover's ownership-graph cross-validation uses
 * internally, surfaced here per-hospital instead. Requires
 * POST /api/pipeline/ingest-ownership (Dashboard → "Run ownership
 * ingest") to have run at least once, or this is always empty — unless
 * `no_stake_expected` says this hospital's ownership category (e.g.
 * Department of Defense) structurally carries no PECOS disclosure at
 * all, in which case an empty list gets a different, non-actionable
 * message instead of the "go run ingest" nudge.
 *
 * Split into two tables: actual ownership/partnership/mortgage-debt
 * interest (has a percentage), versus management/control roles (corporate
 * officers, directors, W-2 managing employees) — CMS's PECOS data lists
 * both as "owners" but they're not the same thing. */
export function OwnershipPanel({ facilityId }: { facilityId: string }) {
  const { data, isLoading, error } = useHospitalOwnership(facilityId)
  const owners = data?.owners
  const noStakeExpected = data?.no_stake_expected ?? false

  const ownershipRows = owners?.filter(isOwnershipInterestRole) ?? []
  const managementRows = owners?.filter((o) => !isOwnershipInterestRole(o)) ?? []

  return (
    <div className="rounded-lg border border-slate-200 bg-white p-5 dark:border-slate-800 dark:bg-slate-900">
      <h3 className="mb-3 text-sm font-semibold text-slate-900 dark:text-slate-100">Ownership</h3>

      {isLoading && <p className="text-sm text-slate-500 dark:text-slate-400">Loading…</p>}
      {error && <p className="text-sm text-red-600 dark:text-red-400">{error.message}</p>}

      {owners && owners.length === 0 && noStakeExpected && (
        <p className="text-sm text-slate-500 dark:text-slate-400">
          This hospital's ownership type doesn't support private ownership, so CMS's PECOS data has no
          owner to disclose here — this is expected, not missing data.
        </p>
      )}

      {owners && owners.length === 0 && !noStakeExpected && (
        <p className="text-sm text-slate-500 dark:text-slate-400">
          No ownership data on file for this hospital. Either CMS hasn't disclosed an owner for it,
          or ownership data hasn't been ingested yet — see{' '}
          <Link to="/" className="text-sky-700 hover:underline dark:text-sky-400">
            Dashboard → Run ownership ingest
          </Link>
          .
        </p>
      )}

      {owners && owners.length > 0 && (
        <div className="flex flex-col gap-5">
          <div>
            <h4 className="mb-2 text-xs font-semibold uppercase tracking-wide text-slate-500 dark:text-slate-400">
              Ownership interest
            </h4>
            {ownershipRows.length > 0 ? (
              <OwnerTable owners={ownershipRows} showPercentage />
            ) : (
              <p className="text-sm text-slate-500 dark:text-slate-400">
                No financial ownership/partnership interest disclosed.
              </p>
            )}
          </div>

          <div>
            <h4 className="mb-2 text-xs font-semibold uppercase tracking-wide text-slate-500 dark:text-slate-400">
              Management &amp; control
            </h4>
            {managementRows.length > 0 ? (
              <ManagementTable owners={managementRows} />
            ) : (
              <p className="text-sm text-slate-500 dark:text-slate-400">
                No corporate officers, directors, or managing employees disclosed.
              </p>
            )}
          </div>
        </div>
      )}
    </div>
  )
}
