import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";
import { URL, fileURLToPath } from "node:url";
const workspaceRoot = fileURLToPath(new URL("..", import.meta.url));
export default defineConfig({
    plugins: [react()],
    server: {
        host: "0.0.0.0",
        port: 5174,
        fs: {
            allow: [workspaceRoot]
        }
    },
    preview: {
        host: "0.0.0.0",
        port: 4174
    }
});
