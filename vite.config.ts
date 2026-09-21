import { defineConfig } from 'vite';
import dts from 'vite-plugin-dts';

export default defineConfig({
  build: {
    lib: {
      entry: 'src/index.ts',
      formats: ['es', 'cjs'],
      fileName: (format) => (format === 'es' ? 'index.js' : 'index.cjs'),
    },
    rollupOptions: {
      // Bundle SSignal because its published CommonJS export uses a `.cjs.js` file while
      // declaring `type: module`, which Node cannot require directly.
      external: ['express', 'node:crypto'],
    },
    sourcemap: true,
  },
  plugins: [dts({ entryRoot: 'src', include: ['src'] })],
});
