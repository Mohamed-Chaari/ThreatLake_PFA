import react from "@vitejs/plugin-react";
import { defineConfig } from "vite";

// Proxies /alerts, /attacker_profiles, and /copilot to the FastAPI server
// (threatlake.api.app, run separately with uvicorn on port 8000) so the
// dashboard's own fetch calls can use plain relative paths - no CORS
// configuration needed on either side during local development.
export default defineConfig({
  plugins: [react()],
  server: {
    proxy: {
      "/alerts": "http://127.0.0.1:8000",
      "/attacker_profiles": "http://127.0.0.1:8000",
      "/copilot": "http://127.0.0.1:8000",
    },
  },
});
