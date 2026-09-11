import React from 'react';
import { Building2, MapPin, Award, Calendar, ArrowRight } from 'lucide-react';
import { Badge } from '../common/Badge';
import { Button } from '../common/Button';
import { formatDate } from '../../utils/formatters';
import type { HospitalSummary } from '../../api/types';

interface HospitalCardProps {
  hospital: HospitalSummary;
  onSelect: (hospitalId: number) => void;
}

export const HospitalCard: React.FC<HospitalCardProps> = ({
  hospital,
  onSelect,
}) => {
  return (
    <div className="bg-white dark:bg-slate-900 rounded-xl border border-slate-200 dark:border-slate-800 p-5 shadow-sm hover:border-brand-300 dark:hover:border-brand-700 transition-all text-left flex flex-col justify-between group">
      <div>
        {/* Top Header Row */}
        <div className="flex items-start justify-between gap-3 mb-2">
          <div className="h-10 w-10 rounded-lg bg-brand-50 dark:bg-brand-950/60 text-brand-600 dark:text-brand-400 flex items-center justify-center shrink-0 border border-brand-100 dark:border-brand-900">
            <Building2 className="h-5 w-5" />
          </div>
          <div className="flex items-center gap-1.5 flex-wrap justify-end">
            {hospital.license_number && (
              <Badge variant="neutral" size="sm" title="CMS Certification or State License">
                <Award className="h-3 w-3" />
                CMS ID: {hospital.license_number}
              </Badge>
            )}
            {hospital.version && (
              <Badge variant="brand" size="sm">
                v{hospital.version}
              </Badge>
            )}
          </div>
        </div>

        {/* Hospital Name */}
        <h3 className="font-bold text-base text-slate-900 dark:text-slate-100 group-hover:text-brand-600 dark:group-hover:text-brand-400 transition-colors">
          {hospital.hospital_name}
        </h3>

        {/* Address */}
        <p className="flex items-center gap-1.5 text-xs text-slate-500 dark:text-slate-400 mt-1.5">
          <MapPin className="h-3.5 w-3.5 shrink-0 text-slate-400" />
          <span>
            {[hospital.hospital_address, hospital.hospital_city, hospital.hospital_state]
              .filter(Boolean)
              .join(', ') || 'Address on file'}
          </span>
        </p>
      </div>

      {/* Footer Info & Action */}
      <div className="mt-4 pt-3 border-t border-slate-100 dark:border-slate-800 flex items-center justify-between text-xs text-slate-400">
        <span className="flex items-center gap-1">
          <Calendar className="h-3 w-3" />
          Updated {formatDate(hospital.last_updated_on)}
        </span>

        <Button
          variant="ghost"
          size="sm"
          onClick={() => onSelect(hospital.hospital_id)}
          className="text-xs group-hover:text-brand-600 dark:group-hover:text-brand-400"
          icon={<ArrowRight className="h-3.5 w-3.5" />}
        >
          View Charges
        </Button>
      </div>
    </div>
  );
};
