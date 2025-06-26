import { defineConfig, getDefaultConfig } from './src/index.ts';

/** @type {import('tsup').Options} */
const options = {
    ...getDefaultConfig(),
    entry: ['src/index.ts'],
};

export default defineConfig([options]);
