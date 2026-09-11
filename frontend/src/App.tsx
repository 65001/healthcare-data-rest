import React, { useState, useEffect } from 'react';
import { Header, type NavTab } from './components/common/Header';
import { Footer } from './components/common/Footer';
import { SkipLink } from './components/common/SkipLink';
import { ComparePage } from './pages/ComparePage';
import { HospitalsPage } from './pages/HospitalsPage';
import { ProceduresPage } from './pages/ProceduresPage';
import { AboutPage } from './pages/AboutPage';
import { DataSourcesPage } from './pages/DataSourcesPage';
import { HospitalDetailView } from './components/hospitals/HospitalDetailView';

const getInitialTab = (): NavTab => {
  if (typeof window === 'undefined') return 'compare';
  const path = window.location.pathname.replace(/^\//, '').toLowerCase();
  const hash = window.location.hash.replace(/^#\/?/, '').toLowerCase();
  const route = hash || path;
  if (route.startsWith('hospital')) return 'hospitals';
  if (route.startsWith('procedure')) return 'procedures';
  if (route.startsWith('source') || route.startsWith('data-source')) return 'sources';
  if (route.startsWith('about')) return 'about';
  return 'compare';
};

export const App: React.FC = () => {
  const [activeTab, setActiveTab] = useState<NavTab>(getInitialTab);
  const [selectedHospitalId, setSelectedHospitalId] = useState<number | null>(null);
  const [compareCode, setCompareCode] = useState<string>('99213');

  // Sync tab with URL hash
  const handleTabChange = (tab: NavTab) => {
    setActiveTab(tab);
    window.location.hash = tab;
  };

  useEffect(() => {
    const handleHashChange = () => {
      setActiveTab(getInitialTab());
    };
    window.addEventListener('hashchange', handleHashChange);
    return () => window.removeEventListener('hashchange', handleHashChange);
  }, []);

  // Dark mode toggle management
  const [isDark, setIsDark] = useState(() => {
    if (typeof window !== 'undefined') {
      const saved = localStorage.getItem('careprice-theme');
      if (saved) return saved === 'dark';
      return window.matchMedia('(prefers-color-scheme: dark)').matches;
    }
    return false;
  });

  useEffect(() => {
    const root = document.documentElement;
    if (isDark) {
      root.classList.add('dark');
      localStorage.setItem('careprice-theme', 'dark');
    } else {
      root.classList.remove('dark');
      localStorage.setItem('careprice-theme', 'light');
    }
  }, [isDark]);

  const handleToggleTheme = () => setIsDark((prev) => !prev);

  // Jump from procedure row or hospital charge to Compare tab with prefilled code
  const handleCompareProcedure = (code: string) => {
    setCompareCode(code);
    handleTabChange('compare');
    window.scrollTo({ top: 0, behavior: 'smooth' });
  };

  return (
    <div className="min-h-screen flex flex-col bg-slate-50 dark:bg-slate-950 text-slate-900 dark:text-slate-100 transition-colors">
      <SkipLink />

      {/* Main App Header */}
      <Header
        activeTab={activeTab}
        onTabChange={handleTabChange}
        isDark={isDark}
        onToggleTheme={handleToggleTheme}
      />

      {/* Main Content Landmark */}
      <main id="main-content" className="flex-1 w-full px-4 sm:px-6 lg:px-8 py-8">
        {activeTab === 'compare' && (
          <ComparePage
            initialCode={compareCode}
            onSelectHospital={(id) => setSelectedHospitalId(id)}
          />
        )}

        {activeTab === 'hospitals' && (
          <HospitalsPage
            onSelectHospital={(id) => setSelectedHospitalId(id)}
          />
        )}

        {activeTab === 'procedures' && (
          <ProceduresPage
            onCompare={handleCompareProcedure}
            onSelectHospital={(id) => setSelectedHospitalId(id)}
          />
        )}

        {activeTab === 'sources' && <DataSourcesPage />}

        {activeTab === 'about' && <AboutPage />}
      </main>

      {/* Global Hospital Detail Modal Dialog */}
      <HospitalDetailView
        hospitalId={selectedHospitalId}
        isOpen={selectedHospitalId !== null}
        onClose={() => setSelectedHospitalId(null)}
        onCompareProcedure={handleCompareProcedure}
      />

      {/* Accessibility Compliant Footer */}
      <Footer />
    </div>
  );
};
