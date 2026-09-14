import { createColumnHelper, getCoreRowModel, useReactTable, flexRender } from '@tanstack/react-table'
import type { NeedsEnrichmentItem } from '../lib/types'
import { EnrichmentEditor } from './EnrichmentEditor'

const columnHelper = createColumnHelper<NeedsEnrichmentItem>()

const columns = [
  columnHelper.accessor('facility_name', {
    header: 'Facility',
    cell: (info) => (
      <div>
        <div className="font-medium text-slate-900">{info.getValue()}</div>
        <div className="text-xs text-slate-500">{info.row.original.facility_id}</div>
      </div>
    ),
  }),
  columnHelper.display({
    id: 'address',
    header: 'Address',
    cell: (info) => {
      const h = info.row.original
      return (
        <div className="text-slate-600">
          {h.address}
          <br />
          {h.city}, {h.state} {h.zip_code}
        </div>
      )
    },
  }),
  columnHelper.display({
    id: 'missing',
    header: 'Missing',
    cell: (info) => {
      const h = info.row.original
      return (
        <div className="flex flex-col gap-1">
          {h.missing_coordinates && (
            <span className="inline-flex items-center rounded-full bg-amber-100 px-2 py-0.5 text-xs font-medium text-amber-800">
              Coordinates
            </span>
          )}
          {h.missing_website && (
            <span className="inline-flex items-center rounded-full bg-amber-100 px-2 py-0.5 text-xs font-medium text-amber-800">
              Website
            </span>
          )}
        </div>
      )
    },
  }),
  columnHelper.display({
    id: 'enrichment',
    header: 'Manual enrichment',
    cell: (info) => <EnrichmentEditor item={info.row.original} />,
  }),
]

export function EnrichmentQueueTable({ data }: { data: NeedsEnrichmentItem[] }) {
  const table = useReactTable({
    data,
    columns,
    getCoreRowModel: getCoreRowModel(),
    getRowId: (row) => row.facility_id,
  })

  if (data.length === 0) {
    return (
      <div className="rounded-lg border border-dashed border-slate-300 bg-white py-12 text-center text-sm text-slate-500">
        Nothing in the queue for these filters — every enriched hospital has both coordinates and a
        website.
      </div>
    )
  }

  return (
    <div className="overflow-x-auto rounded-lg border border-slate-200 bg-white">
      <table className="w-full text-sm">
        <thead className="border-b border-slate-200 bg-slate-50 text-left text-xs font-semibold uppercase tracking-wide text-slate-500">
          {table.getHeaderGroups().map((headerGroup) => (
            <tr key={headerGroup.id}>
              {headerGroup.headers.map((header) => (
                <th key={header.id} className="px-4 py-3">
                  {flexRender(header.column.columnDef.header, header.getContext())}
                </th>
              ))}
            </tr>
          ))}
        </thead>
        <tbody className="divide-y divide-slate-100">
          {table.getRowModel().rows.map((row) => (
            <tr key={row.id} className="align-top">
              {row.getVisibleCells().map((cell) => (
                <td key={cell.id} className="px-4 py-3">
                  {flexRender(cell.column.columnDef.cell, cell.getContext())}
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}
