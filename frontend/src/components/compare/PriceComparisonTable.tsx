import React, { useMemo, useState } from 'react';
import {
  createColumnHelper,
  flexRender,
  getCoreRowModel,
  getPaginationRowModel,
  getSortedRowModel,
  useReactTable,
} from '@tanstack/react-table';
import type { SortingState } from '@tanstack/react-table';
import {
  ArrowUpDown,
  ArrowUp,
  ArrowDown,
  CheckCircle2,
  Download,
  Share2,
  ExternalLink,
  MapPin,
  ChevronLeft,
  ChevronRight,
} from 'lucide-react';
import { Badge } from '../common/Badge';
import { Button } from '../common/Button';
import { formatCurrency } from '../../utils/formatters';
import type { PriceComparisonItem } from '../../api/types';

interface PriceComparisonTableProps {
  items: PriceComparisonItem[];
  onSelectHospital: (hospitalId: number) => void;
}

const columnHelper = createColumnHelper<PriceComparisonItem>();

export const PriceComparisonTable: React.FC<PriceComparisonTableProps> = ({
  items,
  onSelectHospital,
}) => {
  const [sorting, setSorting] = useState<SortingState>([
    { id: 'negotiated_dollar', desc: false }, // default sort by lowest negotiated price
  ]);
  const [copied, setCopied] = useState(false);

  // Find lowest price to highlight the Best Value facility
  const minNegotiatedPrice = useMemo(() => {
    const valid = items
      .map((i) => i.negotiated_dollar)
      .filter((p): p is number => typeof p === 'number' && p > 0);
    return valid.length > 0 ? Math.min(...valid) : null;
  }, [items]);

  const columns = useMemo(
    () => [
      columnHelper.accessor('hospital_name', {
        header: 'Hospital / Facility',
        cell: (info) => {
          const row = info.row.original;
          const isBestValue =
            row.negotiated_dollar !== null &&
            row.negotiated_dollar !== undefined &&
            row.negotiated_dollar === minNegotiatedPrice;

          return (
            <div className="flex flex-col text-left py-1">
              <div className="flex items-center gap-1.5 flex-wrap">
                <button
                  type="button"
                  onClick={() => onSelectHospital(row.hospital_id)}
                  className="font-semibold text-slate-900 dark:text-slate-100 hover:text-brand-600 dark:hover:text-brand-400 text-sm focus-visible:outline-none focus-visible:underline text-left"
                >
                  {row.hospital_name}
                </button>
                {isBestValue && (
                  <Badge variant="success" size="sm">
                    <CheckCircle2 className="h-3 w-3" />
                    Best Value
                  </Badge>
                )}
              </div>
              <div className="flex items-center gap-1 text-xs text-slate-500 dark:text-slate-400 mt-0.5">
                <MapPin className="h-3 w-3 shrink-0" />
                <span>
                  {[row.hospital_city, row.hospital_state].filter(Boolean).join(', ') || 'USA'}
                </span>
              </div>
            </div>
          );
        },
      }),

      columnHelper.accessor('payer_name', {
        header: 'Insurance Payer / Plan',
        cell: (info) => {
          const row = info.row.original;
          return (
            <div className="text-left py-1">
              <span className="font-medium text-slate-800 dark:text-slate-200 text-xs sm:text-sm block">
                {row.payer_name || 'Standard / Unspecified'}
              </span>
              {row.plan_name && (
                <span className="text-[11px] text-slate-500 dark:text-slate-400 block truncate max-w-xs">
                  {row.plan_name}
                </span>
              )}
            </div>
          );
        },
      }),

      columnHelper.accessor('negotiated_dollar', {
        header: 'Negotiated Rate',
        cell: (info) => {
          const val = info.getValue();
          const isBest =
            val !== null && val !== undefined && val === minNegotiatedPrice;
          return (
            <div className="text-right font-mono py-1">
              <span
                className={`text-sm font-bold ${
                  isBest
                    ? 'text-emerald-600 dark:text-emerald-400 text-base'
                    : 'text-slate-900 dark:text-slate-100'
                }`}
              >
                {formatCurrency(val)}
              </span>
            </div>
          );
        },
      }),

      columnHelper.accessor('discounted_cash', {
        header: 'Discounted Cash Price',
        cell: (info) => (
          <div className="text-right font-mono py-1">
            <span className="text-xs sm:text-sm text-slate-700 dark:text-slate-300">
              {formatCurrency(info.getValue())}
            </span>
          </div>
        ),
      }),

      columnHelper.accessor('gross_charge', {
        header: 'Gross (List) Charge',
        cell: (info) => (
          <div className="text-right font-mono py-1">
            <span className="text-xs text-slate-400 dark:text-slate-500 line-through">
              {formatCurrency(info.getValue())}
            </span>
          </div>
        ),
      }),

      columnHelper.accessor('setting', {
        header: 'Setting',
        cell: (info) => {
          const val = info.getValue();
          if (!val) return <span className="text-xs text-slate-400">—</span>;
          const isOutpatient = val.toLowerCase().includes('outpatient');
          return (
            <Badge
              variant={isOutpatient ? 'info' : 'purple'}
              size="sm"
              className="capitalize"
            >
              {val}
            </Badge>
          );
        },
      }),

      columnHelper.display({
        id: 'actions',
        header: '',
        cell: (info) => (
          <div className="text-right py-1">
            <Button
              variant="ghost"
              size="sm"
              onClick={() => onSelectHospital(info.row.original.hospital_id)}
              className="text-xs"
              icon={<ExternalLink className="h-3 w-3" />}
            >
              View Hospital
            </Button>
          </div>
        ),
      }),
    ],
    [minNegotiatedPrice, onSelectHospital]
  );

  const table = useReactTable({
    data: items,
    columns,
    state: { sorting },
    onSortingChange: setSorting,
    getCoreRowModel: getCoreRowModel(),
    getSortedRowModel: getSortedRowModel(),
    getPaginationRowModel: getPaginationRowModel(),
    initialState: {
      pagination: {
        pageSize: 10,
      },
    },
  });

  const handleExportCSV = () => {
    if (items.length === 0) return;
    const headers = [
      'Hospital Name',
      'City',
      'State',
      'Payer Name',
      'Plan Name',
      'Negotiated Rate ($)',
      'Discounted Cash ($)',
      'Gross Charge ($)',
      'Setting',
      'Methodology',
    ];

    const rows = items.map((i) => [
      `"${(i.hospital_name || '').replace(/"/g, '""')}"`,
      `"${(i.hospital_city || '').replace(/"/g, '""')}"`,
      `"${(i.hospital_state || '').replace(/"/g, '""')}"`,
      `"${(i.payer_name || '').replace(/"/g, '""')}"`,
      `"${(i.plan_name || '').replace(/"/g, '""')}"`,
      i.negotiated_dollar ?? '',
      i.discounted_cash ?? '',
      i.gross_charge ?? '',
      `"${(i.setting || '').replace(/"/g, '""')}"`,
      `"${(i.methodology || '').replace(/"/g, '""')}"`,
    ]);

    const csvContent = [headers.join(','), ...rows.map((r) => r.join(','))].join('\n');
    const blob = new Blob([csvContent], { type: 'text/csv;charset=utf-8;' });
    const url = URL.createObjectURL(blob);
    const link = document.createElement('a');
    link.setAttribute('href', url);
    link.setAttribute('download', `price_comparison_${new Date().toISOString().slice(0, 10)}.csv`);
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
  };

  const handleCopyLink = () => {
    navigator.clipboard.writeText(window.location.href);
    setCopied(true);
    setTimeout(() => setCopied(false), 2500);
  };

  return (
    <div className="w-full bg-white dark:bg-slate-900 rounded-2xl border border-slate-200 dark:border-slate-800 shadow-sm overflow-hidden text-left">
      {/* Table Toolbar */}
      <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-3 p-4 border-b border-slate-100 dark:border-slate-800 bg-slate-50/50 dark:bg-slate-900/50">
        <div>
          <h3 className="text-sm font-bold text-slate-900 dark:text-slate-100">
            Comparative Price Breakdown ({items.length} Quotes)
          </h3>
          <p className="text-xs text-slate-500 dark:text-slate-400">
            Click column headers to sort by rate or facility
          </p>
        </div>

        <div className="flex items-center gap-2">
          <Button
            variant="outline"
            size="sm"
            onClick={handleCopyLink}
            icon={<Share2 className="h-3.5 w-3.5" />}
          >
            {copied ? 'Link Copied!' : 'Share'}
          </Button>

          <Button
            variant="outline"
            size="sm"
            onClick={handleExportCSV}
            icon={<Download className="h-3.5 w-3.5" />}
          >
            Export CSV
          </Button>
        </div>
      </div>

      {/* TanStack Table Desktop Container */}
      <div className="overflow-x-auto">
        <table className="w-full text-left text-sm border-collapse" aria-label="Procedure Price Comparison Table">
          <caption className="sr-only">Hospital price comparison results across payers</caption>
          <thead className="bg-slate-50 dark:bg-slate-800/60 text-xs font-semibold text-slate-600 dark:text-slate-300 border-b border-slate-200 dark:border-slate-700">
            {table.getHeaderGroups().map((headerGroup) => (
              <tr key={headerGroup.id}>
                {headerGroup.headers.map((header) => {
                  const isSortable = header.column.getCanSort();
                  const sortDirection = header.column.getIsSorted();
                  return (
                    <th
                      key={header.id}
                      scope="col"
                      className="py-3 px-4 select-none whitespace-nowrap"
                    >
                      {isSortable ? (
                        <button
                          type="button"
                          onClick={header.column.getToggleSortingHandler()}
                          className="flex items-center gap-1.5 hover:text-slate-900 dark:hover:text-white focus-visible:outline-none focus-visible:ring-1 focus-visible:ring-brand-500 rounded"
                          aria-label={`Sort by ${String(header.column.columnDef.header)}`}
                        >
                          <span>
                            {flexRender(
                              header.column.columnDef.header,
                              header.getContext()
                            )}
                          </span>
                          {sortDirection === 'asc' ? (
                            <ArrowUp className="h-3.5 w-3.5 text-brand-600" />
                          ) : sortDirection === 'desc' ? (
                            <ArrowDown className="h-3.5 w-3.5 text-brand-600" />
                          ) : (
                            <ArrowUpDown className="h-3.5 w-3.5 text-slate-400 opacity-60" />
                          )}
                        </button>
                      ) : (
                        flexRender(
                          header.column.columnDef.header,
                          header.getContext()
                        )
                      )}
                    </th>
                  );
                })}
              </tr>
            ))}
          </thead>
          <tbody className="divide-y divide-slate-100 dark:divide-slate-800">
            {table.getRowModel().rows.length === 0 ? (
              <tr>
                <td colSpan={columns.length} className="text-center py-10 text-slate-500 dark:text-slate-400">
                  No pricing records found matching your filters.
                </td>
              </tr>
            ) : (
              table.getRowModel().rows.map((row) => (
                <tr
                  key={row.id}
                  className="hover:bg-slate-50/70 dark:hover:bg-slate-800/40 transition-colors"
                >
                  {row.getVisibleCells().map((cell) => (
                    <td key={cell.id} className="py-3 px-4">
                      {flexRender(cell.column.columnDef.cell, cell.getContext())}
                    </td>
                  ))}
                </tr>
              ))
            )}
          </tbody>
        </table>
      </div>

      {/* Pagination Controls */}
      {items.length > 10 && (
        <div className="flex items-center justify-between border-t border-slate-200 dark:border-slate-800 p-4">
          <Button
            variant="outline"
            size="sm"
            disabled={!table.getCanPreviousPage()}
            onClick={() => table.previousPage()}
            icon={<ChevronLeft className="h-4 w-4" />}
          >
            Previous
          </Button>

          <span className="text-xs font-medium text-slate-600 dark:text-slate-400">
            Page {table.getState().pagination.pageIndex + 1} of {table.getPageCount() || 1}
          </span>

          <Button
            variant="outline"
            size="sm"
            disabled={!table.getCanNextPage()}
            onClick={() => table.nextPage()}
          >
            <span>Next</span>
            <ChevronRight className="h-4 w-4 ml-1" />
          </Button>
        </div>
      )}
    </div>
  );
};
