import { defineConfig } from 'vitest/config'
import { fileURLToPath } from 'node:url'
export default defineConfig({
  resolve: {
    alias: {
      'easy-rpc/protocol': fileURLToPath(new URL('./src/protocol.ts', import.meta.url)),
      'easy-rpc/server': fileURLToPath(new URL('./src/server.ts', import.meta.url)),
    },
  },
  test: { include: ['tests/**/*.test.ts'] },
})
