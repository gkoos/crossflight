import { defineConfig } from 'tsup'

export default defineConfig({
  entry: [
    'src/index.ts',
    'src/coordinators/redis.ts',
    'src/adapters/keyv.ts',
    'src/adapters/cache-manager.ts',
    'src/adapters/cacheable.ts',
  ],
  format: ['esm', 'cjs'],
  dts: true,
  sourcemap: true,
  clean: true,
  outDir: 'dist',
})
