import React, { useState } from 'react';
import {
  Activity,
  Building2,
  Database,
  Moon,
  Sun,
  Search,
  Scale,
  Menu,
  X,
  FileText,
} from 'lucide-react';
import { useDatasetStats } from '../../api/hooks/useStats';
import { formatNumber } from '../../utils/formatters';

export type NavTab = 'compare' | 'hospitals' | 'procedures' | 'sources' | 'about';

interface HeaderProps {
  activeTab: NavTab;
  onTabChange: (tab: NavTab) => void;
  isDark: boolean;
  onToggleTheme: () => void;
}

export const Header: React.FC<HeaderProps> = ({
  activeTab,
  onTabChange,
  isDark,
  onToggleTheme,
}) => {
  const [mobileMenuOpen, setMobileMenuOpen] = useState(false);
  const { data: stats, isLoading: statsLoading } = useDatasetStats();

  const navItems = [
    { id: 'compare' as NavTab, label: 'Compare Prices', icon: Scale },
    { id: 'hospitals' as NavTab, label: 'Hospitals', icon: Building2 },
    { id: 'procedures' as NavTab, label: 'Procedures', icon: Search },
    { id: 'sources' as NavTab, label: 'Data Sources & Replication', icon: Database },
    { id: 'about' as NavTab, label: 'About', icon: FileText },
  ];

  return (
    <header className="sticky top-0 z-40 w-full border-b border-slate-200 dark:border-slate-800 bg-white/95 dark:bg-slate-900/95 backdrop-blur supports-[backdrop-filter]:bg-white/80 dark:supports-[backdrop-filter]:bg-slate-900/80">
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
        <div className="flex h-16 items-center justify-between gap-4">
          {/* Brand / Logo */}
          <div className="flex items-center gap-3">
            <button
              onClick={() => onTabChange('compare')}
              className="flex items-center gap-2.5 text-left focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-brand-500 rounded-lg p-1"
              aria-label="CarePrice Home"
            >
              <div className="h-9 w-9 rounded-xl bg-gradient-to-tr from-brand-600 to-sky-400 flex items-center justify-center text-white shadow-sm shadow-brand-500/30">
                <Activity className="h-5 w-5" />
              </div>
              <div>
                <span className="text-lg font-extrabold tracking-tight bg-gradient-to-r from-brand-700 via-brand-600 to-sky-600 dark:from-brand-400 dark:to-sky-300 bg-clip-text text-transparent">
                  CarePrice
                </span>
                <span className="hidden sm:inline-block ml-2 text-xs font-semibold px-2 py-0.5 rounded bg-slate-100 dark:bg-slate-800 text-slate-600 dark:text-slate-300">
                  Transparency
                </span>
              </div>
            </button>
          </div>

          {/* Desktop Navigation */}
          <nav className="hidden md:flex items-center gap-1" aria-label="Main Navigation">
            {navItems.map((item) => {
              const Icon = item.icon;
              const isActive = activeTab === item.id;
              return (
                <button
                  key={item.id}
                  onClick={() => onTabChange(item.id)}
                  aria-current={isActive ? 'page' : undefined}
                  className={`flex items-center gap-2 px-3.5 py-2 text-sm font-medium rounded-lg transition-colors focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-brand-500 ${
                    isActive
                      ? 'bg-brand-50 text-brand-700 dark:bg-brand-950/80 dark:text-brand-300 font-semibold'
                      : 'text-slate-600 hover:text-slate-900 hover:bg-slate-50 dark:text-slate-300 dark:hover:text-white dark:hover:bg-slate-800'
                  }`}
                >
                  <Icon className="h-4 w-4" aria-hidden="true" />
                  <span>{item.label}</span>
                </button>
              );
            })}
          </nav>

          {/* Right Action Bar */}
          <div className="flex items-center gap-2.5">
            {/* DuckLake Health Indicator */}
            <div className="hidden lg:flex items-center gap-2 text-xs text-slate-500 dark:text-slate-400 border-r border-slate-200 dark:border-slate-800 pr-3">
              <Database className="h-3.5 w-3.5 text-slate-400" aria-hidden="true" />
              {statsLoading ? (
                <span>Connecting...</span>
              ) : stats?.is_lake_attached ? (
                <span className="inline-flex items-center gap-1.5 font-medium text-emerald-600 dark:text-emerald-400">
                  <span className="h-2 w-2 rounded-full bg-emerald-500 animate-pulse" />
                  DuckLake ({formatNumber(stats.total_hospitals)} hospitals)
                </span>
              ) : (
                <span className="inline-flex items-center gap-1.5 font-medium text-slate-500">
                  <span className="h-2 w-2 rounded-full bg-slate-400" />
                  Lake Offline
                </span>
              )}
            </div>

            {/* Theme Toggle Button */}
            <button
              onClick={onToggleTheme}
              className="p-2 rounded-lg text-slate-500 hover:text-slate-900 hover:bg-slate-100 dark:text-slate-400 dark:hover:text-slate-100 dark:hover:bg-slate-800 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-brand-500 transition-colors"
              aria-label={isDark ? 'Switch to light theme' : 'Switch to dark theme'}
            >
              {isDark ? (
                <Sun className="h-4 w-4" aria-hidden="true" />
              ) : (
                <Moon className="h-4 w-4" aria-hidden="true" />
              )}
            </button>

            {/* Mobile Menu Hamburger */}
            <button
              onClick={() => setMobileMenuOpen(!mobileMenuOpen)}
              className="md:hidden p-2 rounded-lg text-slate-600 hover:bg-slate-100 dark:text-slate-300 dark:hover:bg-slate-800 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-brand-500"
              aria-label={mobileMenuOpen ? 'Close menu' : 'Open menu'}
              aria-expanded={mobileMenuOpen}
            >
              {mobileMenuOpen ? <X className="h-5 w-5" /> : <Menu className="h-5 w-5" />}
            </button>
          </div>
        </div>
      </div>

      {/* Mobile Navigation Drawer */}
      {mobileMenuOpen && (
        <div className="md:hidden border-b border-slate-200 dark:border-slate-800 bg-white dark:bg-slate-900 px-4 pt-2 pb-4 space-y-1">
          {navItems.map((item) => {
            const Icon = item.icon;
            const isActive = activeTab === item.id;
            return (
              <button
                key={item.id}
                onClick={() => {
                  onTabChange(item.id);
                  setMobileMenuOpen(false);
                }}
                className={`w-full flex items-center gap-3 px-3 py-2.5 text-base font-medium rounded-lg text-left transition-colors ${
                  isActive
                    ? 'bg-brand-50 text-brand-700 dark:bg-brand-950 dark:text-brand-300 font-semibold'
                    : 'text-slate-700 hover:bg-slate-50 dark:text-slate-200 dark:hover:bg-slate-800'
                }`}
              >
                <Icon className="h-5 w-5" aria-hidden="true" />
                <span>{item.label}</span>
              </button>
            );
          })}
          {stats && (
            <div className="pt-2 mt-2 border-t border-slate-100 dark:border-slate-800 text-xs text-slate-500 px-3 flex items-center justify-between">
              <span>DuckLake Status:</span>
              <span className="font-semibold text-emerald-600 dark:text-emerald-400">
                {stats.is_lake_attached ? 'Connected' : 'Offline'} ({formatNumber(stats.total_hospitals)} hospitals)
              </span>
            </div>
          )}
        </div>
      )}
    </header>
  );
};
