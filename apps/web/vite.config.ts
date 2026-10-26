import { defineConfig } from 'vitest/config';
import react from '@vitejs/plugin-react';
import tailwindcss from '@tailwindcss/vite';
import { VitePWA } from 'vite-plugin-pwa';

export default defineConfig({
  server: { proxy: { '/api/market': { target: 'http://127.0.0.1:8788' } } },
  plugins: [
    react(),
    tailwindcss(),
    VitePWA({
      registerType: 'prompt',
      injectRegister: null,
      includeManifestIcons: false,
      manifest: {
        id: '/',
        name: 'Sora · 投资，清晰可见',
        short_name: 'Sora',
        description: '发现基金，记录关注与组合。观察市场方向，发现相关基金。',
        lang: 'zh-CN',
        start_url: '/',
        scope: '/',
        display: 'standalone',
        background_color: '#f7f9fc',
        theme_color: '#182b49',
        icons: [
          { src: '/icons/icon-192.png', sizes: '192x192', type: 'image/png' },
          { src: '/icons/icon-512.png', sizes: '512x512', type: 'image/png' },
          {
            src: '/icons/maskable-512.png',
            sizes: '512x512',
            type: 'image/png',
            purpose: 'maskable',
          },
        ],
      },
      workbox: {
        cacheId: 'sora-v2',
        globPatterns: ['**/*.{js,css,html,json,png,svg}'],
        globIgnores: ['**/data/market.json'],
        navigateFallback: '/index.html',
        navigateFallbackDenylist: [
          /^\/(?:data|assets|icons|api)(?:\/|$)/,
          /^\/sw\.js$/,
        ],
        cleanupOutdatedCaches: true,
        skipWaiting: false,
        clientsClaim: false,
      },
      devOptions: { enabled: false },
    }),
  ],
  test: {
    environment: 'jsdom',
    setupFiles: './src/test/setup.ts',
    restoreMocks: true,
  },
});
