import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

// NOTE: the realtime WebSocket host comes from VITE_INNGEST_DEV in `.env`
// (Vite injects VITE_* into import.meta.env, which @inngest/realtime reads
// dynamically in its env.mjs — a `define` for the literal property path
// would NOT apply). Without it the client silently connects to the default
// localhost:8288 while this example's server publishes to 8299: the header
// reads "live" but no chunk ever arrives.

export default defineConfig({
  plugins: [react()],
  server: {
    port: 5173,
    proxy: {
      "/api": {
        target: "http://localhost:8484",
        changeOrigin: true,
      },
    },
  },
});
