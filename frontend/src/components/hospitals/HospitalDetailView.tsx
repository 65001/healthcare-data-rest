import React, { useState } from 'react';
import {
  MapPin,
  ExternalLink,
  Award,
  Search,
  ChevronLeft,
  ChevronRight,
  Scale,
} from 'lucide-react';
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
  DialogDescription,
} from '../ui/dialog';
import { useHospital } from '../../api/hooks/useHospitals';
import { useProcedures } from '../../api/hooks/useProcedures';
import { Badge } from '../common/Badge';
import { Button } from '../common/Button';
import { Input } from '../common/Input';
import { Skeleton } from '../common/Skeleton';
import { formatCurrency, formatDate } from '../../utils/formatters';

interface HospitalDetailViewProps {
  hospitalId: number | null;
  isOpen: boolean;
  onClose: () => void;
  onCompareProcedure: (code: string) => void;
}

export const HospitalDetailView: React.FC<HospitalDetailViewProps> = ({
  hospitalId,
  isOpen,
  onClose,
  onCompareProcedure,
}) => {
  const { data: hospital, isLoading: hospitalLoading } = useHospital(hospitalId);
  const [chargeQuery, setChargeQuery] = useState('');
  const [chargeOffset, setChargeOffset] = useState(0);

  const { data: proceduresData, isLoading: proceduresLoading } = useProcedures({
    hospital_id: hospitalId || undefined,
    q: chargeQuery || undefined,
    limit: 10,
    offset: chargeOffset,
  });

  return (
    <Dialog open={isOpen} onOpenChange={(open) => !open && onClose()}>
      <DialogContent className="max-w-4xl text-left">
        {hospitalLoading ? (
          <div className="space-y-4 p-4">
            <Skeleton className="h-8 w-3/4" />
            <Skeleton className="h-4 w-1/2" />
            <Skeleton className="h-28 w-full" />
            <Skeleton className="h-64 w-full" />
          </div>
        ) : !hospital ? (
          <div className="p-8 text-center text-slate-500">
            Hospital details could not be loaded.
          </div>
        ) : (
          <>
            <DialogHeader>
              <div className="flex items-center gap-2 flex-wrap">
                <DialogTitle className="text-xl font-bold">
                  {hospital.hospital_name}
                </DialogTitle>
                {hospital.license_number && (
                  <Badge variant="neutral" size="sm">
                    <Award className="h-3 w-3" />
                    CMS ID: {hospital.license_number}
                  </Badge>
                )}
                {hospital.version && (
                  <Badge variant="brand" size="sm">
                    MRF Schema v{hospital.version}
                  </Badge>
                )}
              </div>
              <DialogDescription className="flex items-center gap-1.5 text-xs text-slate-500 dark:text-slate-400 mt-1">
                <MapPin className="h-3.5 w-3.5 shrink-0" />
                <span>
                  {[hospital.hospital_address, hospital.hospital_city, hospital.hospital_state]
                    .filter(Boolean)
                    .join(', ') || 'No street address on file'}
                </span>
                {hospital.last_updated_on && (
                  <span>• Last Updated: {formatDate(hospital.last_updated_on)}</span>
                )}
              </DialogDescription>
            </DialogHeader>

            {/* Compliance & Policy Badges */}
            <div className="grid grid-cols-1 sm:grid-cols-2 gap-3 p-4 rounded-xl bg-slate-50 dark:bg-slate-800/50 border border-slate-200 dark:border-slate-800 text-xs">
              <div>
                <span className="font-semibold text-slate-700 dark:text-slate-300 block mb-1">
                  CMS Compliance Affirmation
                </span>
                <p className="text-slate-500 dark:text-slate-400 text-[11px] leading-relaxed">
                  {hospital.affirmation || 'Hospital affirmation filed under CMS Price Transparency guidelines.'}
                </p>
              </div>
              <div>
                <span className="font-semibold text-slate-700 dark:text-slate-300 block mb-1">
                  Financial Aid Policy & Provisions
                </span>
                {hospital.financial_aid_policy ? (
                  <a
                    href={hospital.financial_aid_policy}
                    target="_blank"
                    rel="noreferrer"
                    className="inline-flex items-center gap-1 text-brand-600 dark:text-brand-400 hover:underline"
                  >
                    View Official Financial Aid Policy
                    <ExternalLink className="h-3 w-3" />
                  </a>
                ) : (
                  <span className="text-slate-400">Refer to main hospital financial services office.</span>
                )}
              </div>
            </div>

            {/* Standard Charges & Procedures Browser */}
            <div className="mt-4 space-y-3">
              <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-3">
                <h4 className="text-sm font-bold text-slate-900 dark:text-slate-100">
                  Standard Charges & Negotiated Procedure Rates
                </h4>
                <div className="w-full sm:w-64">
                  <Input
                    placeholder="Search procedures at this hospital..."
                    value={chargeQuery}
                    onChange={(e) => {
                      setChargeQuery(e.target.value);
                      setChargeOffset(0);
                    }}
                    icon={<Search className="h-3.5 w-3.5" />}
                    className="py-1 text-xs"
                  />
                </div>
              </div>

              {/* Table of Procedures */}
              <div className="border border-slate-200 dark:border-slate-800 rounded-xl overflow-hidden max-h-72 overflow-y-auto">
                {proceduresLoading ? (
                  <div className="p-6 space-y-2">
                    <Skeleton className="h-8 w-full" />
                    <Skeleton className="h-8 w-full" />
                    <Skeleton className="h-8 w-full" />
                  </div>
                ) : !proceduresData || proceduresData.items.length === 0 ? (
                  <div className="py-8 text-center text-xs text-slate-500">
                    No matching standard charges found for this facility.
                  </div>
                ) : (
                  <table className="w-full text-left text-xs border-collapse">
                    <thead className="sticky top-0 bg-slate-100 dark:bg-slate-800 text-slate-600 dark:text-slate-300 font-semibold border-b border-slate-200 dark:border-slate-700">
                      <tr>
                        <th className="py-2.5 px-3">Procedure / Item</th>
                        <th className="py-2.5 px-3">Code</th>
                        <th className="py-2.5 px-3 text-right">Gross Charge</th>
                        <th className="py-2.5 px-3 text-right">Discounted Cash</th>
                        <th className="py-2.5 px-3 text-right">Avg Negotiated</th>
                        <th className="py-2.5 px-3 text-right">Compare</th>
                      </tr>
                    </thead>
                    <tbody className="divide-y divide-slate-100 dark:divide-slate-800">
                      {proceduresData.items.map((proc) => {
                        const code = proc.cpt || proc.ms_drg || proc.hcpcs || '';
                        return (
                          <tr key={proc.charge_id} className="hover:bg-slate-50/60 dark:hover:bg-slate-800/40">
                            <td className="py-2.5 px-3 font-medium text-slate-900 dark:text-slate-100 max-w-xs truncate" title={proc.description}>
                              {proc.description}
                            </td>
                            <td className="py-2.5 px-3 font-mono">
                              {code ? (
                                <Badge variant="brand" size="sm">
                                  {code}
                                </Badge>
                              ) : (
                                <span className="text-slate-400">—</span>
                              )}
                            </td>
                            <td className="py-2.5 px-3 text-right font-mono text-slate-500">
                              {formatCurrency(proc.gross_charge)}
                            </td>
                            <td className="py-2.5 px-3 text-right font-mono text-slate-800 dark:text-slate-200 font-semibold">
                              {formatCurrency(proc.discounted_cash)}
                            </td>
                            <td className="py-2.5 px-3 text-right font-mono text-emerald-600 dark:text-emerald-400 font-semibold">
                              {formatCurrency(proc.avg_negotiated_rate)}
                            </td>
                            <td className="py-2.5 px-3 text-right">
                              {code ? (
                                <Button
                                  variant="ghost"
                                  size="sm"
                                  onClick={() => {
                                    onClose();
                                    onCompareProcedure(code);
                                  }}
                                  className="text-[11px] py-1 px-2"
                                  title={`Compare ${code} prices across all hospitals`}
                                  icon={<Scale className="h-3 w-3 text-brand-600" />}
                                >
                                  Compare
                                </Button>
                              ) : null}
                            </td>
                          </tr>
                        );
                      })}
                    </tbody>
                  </table>
                )}
              </div>

              {/* Procedures Pagination */}
              {proceduresData && proceduresData.total > 10 && (
                <div className="flex items-center justify-between text-xs text-slate-500 pt-1">
                  <span>
                    Showing {proceduresData.offset + 1}–
                    {Math.min(proceduresData.offset + proceduresData.limit, proceduresData.total)} of{' '}
                    {proceduresData.total} charges
                  </span>
                  <div className="flex items-center gap-2">
                    <Button
                      variant="outline"
                      size="sm"
                      disabled={chargeOffset === 0}
                      onClick={() => setChargeOffset(Math.max(0, chargeOffset - 10))}
                      icon={<ChevronLeft className="h-3 w-3" />}
                    >
                      Prev
                    </Button>
                    <Button
                      variant="outline"
                      size="sm"
                      disabled={chargeOffset + 10 >= proceduresData.total}
                      onClick={() => setChargeOffset(chargeOffset + 10)}
                    >
                      Next
                      <ChevronRight className="h-3 w-3 ml-1" />
                    </Button>
                  </div>
                </div>
              )}
            </div>
          </>
        )}
      </DialogContent>
    </Dialog>
  );
};
