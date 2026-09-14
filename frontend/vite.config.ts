import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'
import tailwindcss from '@tailwindcss/vite'

// Dev server proxies /api to the Axum backend (see backend/src/config.rs —
// defaults to 0.0.0.0:3000, overridable via SERVER_HOST/SERVER_PORT) so the
// app can call same-origin `/api/...` paths with no CORS setup on either
// side. Override the target with VITE_API_PROXY_TARGET if the backend runs
// somewhere else.
const apiProxyTarget = process.env.VITE_API_PROXY_TARGET ?? 'http://localhost:3000'

export default defineConfig({
  plugins: [react(), tailwindcss()],
  server: {
    port: 5173,
    proxy: {
      '/api': {
        target: apiProxyTarget,
        changeOrigin: true,
      },
    },
  },
})
