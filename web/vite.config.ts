import { defineConfig } from "vite";
import vue from "@vitejs/plugin-vue";

export default defineConfig({
  plugins: [vue()],
  server: {
    port: 15173,
    proxy: { "/demo": "http://127.0.0.1:18080" },
  },
});
