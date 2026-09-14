import { Route, Routes } from 'react-router-dom'
import { Layout } from './components/Layout'
import { DashboardPage } from './pages/DashboardPage'
import { QueuePage } from './pages/QueuePage'
import { HospitalsPage } from './pages/HospitalsPage'
import { HospitalDetailPage } from './pages/HospitalDetailPage'

export function App() {
  return (
    <Routes>
      <Route element={<Layout />}>
        <Route path="/" element={<DashboardPage />} />
        <Route path="/queue" element={<QueuePage />} />
        <Route path="/hospitals" element={<HospitalsPage />} />
        <Route path="/hospitals/:facilityId" element={<HospitalDetailPage />} />
      </Route>
    </Routes>
  )
}
