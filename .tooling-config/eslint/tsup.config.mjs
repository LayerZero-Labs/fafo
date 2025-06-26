import { defineConfig, getDefaultConfig } from '@tooling/tsup-config';

/** @type {import('tsup').Options} */
const options = {
    ...getDefaultConfig(),
    entry: ['src/index.ts', 'src/recommended.ts', 'src/strict.ts'],
};

export default defineConfig([options]);
