import { defineConfig, getDefaultConfig } from '@tooling/tsup-config';

/** @type {import('tsup').Options} */
const options = {
    ...getDefaultConfig(),
    entry: ['src/index.ts'],
};

export default defineConfig([options]);
