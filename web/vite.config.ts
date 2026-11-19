import { fileURLToPath } from 'node:url';

import tailwindcss from '@tailwindcss/vite';
import react from '@vitejs/plugin-react';
import { defineConfig } from 'vitest/config';

// Same routes as the nginx config in production (web/nginx.conf.template):
// the browser only ever talks to one origin, so no CORS is needed.
const proxy = {
  '/api/gateway': { target: 'http://localhost:8080', rewrite: strip('/api/gateway') },
  '/api/balance': { target: 'http://localhost:8081', rewrite: strip('/api/balance') },
  '/api/reconciler': { target: 'http://localhost:8082', rewrite: strip('/api/reconciler') },
};

function strip(prefix: string) {
  return (path: string) => path.slice(prefix.length);
}

export default defineConfig({
  plugins: [react(), tailwindcss()],
  resolve: { alias: { '@': fileURLToPath(new URL('./src', import.meta.url)) } },
  server: { port: 5173, proxy },
  preview: { port: 4173, proxy },
  test: {
    environment: 'jsdom',
    setupFiles: ['./src/test-setup.ts'],
    include: ['src/**/*.test.{ts,tsx}'],
    restoreMocks: true,
  },
});
