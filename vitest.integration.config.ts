import { defineConfig } from 'vitest/config'

export default defineConfig({
  test: {
    environment: 'node',
    globals: true,
    include: ['tests/**/*.integration.test.ts'],
    testTimeout: 30000,
    coverage: {
      provider: 'v8',
      include: ['src/coordinators/**/*.ts'],
      reportsDirectory: './coverage-integration',
      thresholds: {
        statements: 75,
        branches: 65,
        functions: 85,
        lines: 75,
      },
    },
  },
})
