import { useState } from 'react'
import {
  createColumnHelper,
  flexRender,
  getCoreRowModel,
  getSortedRowModel,
  useReactTable,
  type SortingState,
} from '@tanstack/react-table'
import { Link } from 'react-router-dom'
import type { HospitalListItem } from '../lib/types'
import { StatusPill } from './StatusPill'
import { SortIndicator } from './SortIndicator'

const columnHelper = createColumnHelper<HospitalListItem>()

const columns = [
  columnHelper.accessor('facility_name', {
    header: 'Facility',
    cell: (info) => (
      <Link
        to={`/hospitals/${encodeURIComponent(info.row.original.facility_id)}`}
        className="font-medium text-slate-900 hover:underline dark:text-slate-100"
      >
        {info.getValue()}
      </Link>
    ),
  }),
  columnHelper.accessor((row) => `${row.state} ${row.city}`, {
    id: 'location',
    header: 'Location',
    cell: (info) => {
      const h = info.row.original
      return (
        <span className="text-slate-600 dark:text-slate-300">
          {h.city}, {h.state} {h.zip_code}
        </span>
      )
    },
  }),
  columnHelper.accessor('hospital_type', { header: 'Type' }),
  columnHelper.accessor('latitude', {
    id: 'coords',
    header: 'Coordinates',
    sortingFn: 'basic',
    cell: (info) => {
      const h = info.row.original
      return h.latitude != null && h.longitude != null ? (
        <span className="font-mono text-xs text-slate-600 dark:text-slate-300">
          {h.latitude.toFixed(4)}, {h.longitude.toFixed(4)}
        </span>
      ) : (
        <span className="text-xs text-slate-400 dark:text-slate-500">—</span>
      )
    },
  }),
  columnHelper.accessor('website_url', {
    header: 'Website',
    cell: (info) => {
      const url = info.getValue()
      return url ? (
        <a
          href={url}
          target="_blank"
          rel="noreferrer"
          className="text-sky-700 hover:underline dark:text-sky-400"
        >
          {url.replace(/^https?:\/\//, '')}
        </a>
      ) : (
        <span className="text-xs text-slate-400 dark:text-slate-500">—</span>
      )
    },
  }),
  columnHelper.accessor('discovery_status', {
    header: 'MRF status',
    cell: (info) => <StatusPill status={info.getValue()} />,
  }),
]

export function HospitalsTable({ data }: { data: HospitalListItem[] }) {
  const [sorting, setSorting] = useState<SortingState>([])

  const table = useReactTable({
    data,
    columns,
    state: { sorting },
    onSortingChange: setSorting,
    getCoreRowModel: getCoreRowModel(),
    getSortedRowModel: getSortedRowModel(),
    getRowId: (row) => row.facility_id,
  })

  if (data.length === 0) {
    return (
      <div className="rounded-lg border border-dashed border-slate-300 bg-white py-12 text-center text-sm text-slate-500 dark:border-slate-700 dark:bg-slate-900 dark:text-slate-400">
        No hospitals match these filters.
      </div>
    )
  }

  return (
    <div className="overflow-x-auto rounded-lg border border-slate-200 bg-white dark:border-slate-800 dark:bg-slate-900">
      <table className="w-full text-sm">
        <thead className="border-b border-slate-200 bg-slate-50 text-left text-xs font-semibold uppercase tracking-wide text-slate-500 dark:border-slate-800 dark:bg-slate-800/50 dark:text-slate-400">
          {table.getHeaderGroups().map((headerGroup) => (
            <tr key={headerGroup.id}>
              {headerGroup.headers.map((header) => (
                <th key={header.id} className="px-4 py-3">
                  {header.column.getCanSort() ? (
                    <button
                      type="button"
                      onClick={header.column.getToggleSortingHandler()}
                      className="flex items-center gap-1 hover:text-slate-700 dark:hover:text-slate-200"
                    >
                      {flexRender(header.column.columnDef.header, header.getContext())}
                      <SortIndicator direction={header.column.getIsSorted()} />
                    </button>
                  ) : (
                    flexRender(header.column.columnDef.header, header.getContext())
                  )}
                </th>
              ))}
            </tr>
          ))}
        </thead>
        <tbody className="divide-y divide-slate-100 dark:divide-slate-800">
          {table.getRowModel().rows.map((row) => (
            <tr key={row.id} className="hover:bg-slate-50 dark:hover:bg-slate-800/50">
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
