import { nodeExternalsPlugin } from 'esbuild-node-externals'
import { defineConfig } from 'tsup'

import type { Options } from 'tsup'

export const getDefaultConfig = (): Partial<Options> => ({
    esbuildPlugins: [
        nodeExternalsPlugin({
            dependencies: false,
            devDependencies: false,
        }),
    ],
    // lets be explicit about module type
    outExtension({ format }): { js: string } {
        if (format === 'cjs') {
            return { js: '.cjs' }
        } else if (format === 'esm') {
            return { js: '.mjs' }
        }
        throw new Error(`Format ${format} is not supported`)
    },
    clean: false,
    dts: true,
    sourcemap: true,
    splitting: false,
    treeshake: true,
    format: ['cjs', 'esm'],
    silent: true,
})

export { defineConfig, Options }
