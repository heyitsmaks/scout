// @lovable.dev/vite-tanstack-config already includes the following — do NOT add them manually
// or the app will break with duplicate plugins:
//   - tanstackStart, viteReact, tailwindcss, tsConfigPaths, nitro (build-only using cloudflare as a default target),
//     componentTagger (dev-only), VITE_* env injection, @ path alias, React/TanStack dedupe,
//     error logger plugins, and sandbox detection (port/host/strictPort).
// You can pass additional config via defineConfig({ vite: { ... }, etc... }) if needed.
import { defineConfig } from "@lovable.dev/vite-tanstack-config";
import { sentryTanstackStart } from "@sentry/tanstackstart-react/vite";
import { VitePWA } from "vite-plugin-pwa";
import { assertBrowserSafeEnvironment } from "./src/lib/browser-key-safety";

export default defineConfig({
  tanstackStart: {
    // Redirect TanStack Start's bundled server entry to src/server.ts (our SSR error wrapper).
    // nitro/vite builds from this
    server: { entry: "server" },
  },
  plugins: [
    {
      name: "scout-browser-key-safety",
      configResolved(config) {
        assertBrowserSafeEnvironment(config.env);
      },
    },
    sentryTanstackStart({
      // No SENTRY_AUTH_TOKEN configured yet, so skip source map upload.
      sourcemaps: { disable: true },
    }),
    VitePWA({
      registerType: "autoUpdate",
      injectRegister: null,
      filename: "sw.js",
      devOptions: { enabled: false },
      workbox: {
        // Take over immediately on update so a republished bundle reaches
        // users on their next navigation, not after every old tab closes.
        skipWaiting: true,
        clientsClaim: true,
        cleanupOutdatedCaches: true,
        navigateFallbackDenylist: [/^\/~oauth/, /^\/api\//],
        runtimeCaching: [
          {
            urlPattern: ({ request }) => request.mode === "navigate",
            handler: "NetworkFirst",
            options: {
              cacheName: "scout-html",
              networkTimeoutSeconds: 5,
            },
          },
          {
            // Code (hashed filenames — a deploy changes every URL). SWR serves
            // instantly from cache but revalidates in the background, so a
            // launch-day hotfix reaches returning users on their next load
            // instead of after a 30-day cache expiry.
            urlPattern: ({ request, url }) =>
              url.origin === self.location.origin &&
              ["style", "script", "worker"].includes(request.destination),
            handler: "StaleWhileRevalidate",
            options: {
              cacheName: "scout-code",
              expiration: { maxEntries: 100, maxAgeSeconds: 60 * 60 * 24 * 7 },
            },
          },
          {
            // Media rarely changes and is heavy — long CacheFirst is right here.
            urlPattern: ({ request, url }) =>
              url.origin === self.location.origin &&
              ["image", "font"].includes(request.destination),
            handler: "CacheFirst",
            options: {
              cacheName: "scout-assets",
              expiration: { maxEntries: 200, maxAgeSeconds: 60 * 60 * 24 * 30 },
            },
          },
        ],
      },
    }),
  ],
});
