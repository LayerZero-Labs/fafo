import { Options } from 'tsup';
export { Options, defineConfig } from 'tsup';

declare const getDefaultConfig: () => Partial<Options>;

export { getDefaultConfig };
