import { nodeExternalsPlugin } from 'esbuild-node-externals';
export { defineConfig } from 'tsup';

// src/index.ts
var getDefaultConfig = () => ({
  esbuildPlugins: [
    nodeExternalsPlugin({
      dependencies: false,
      devDependencies: false
    })
  ],
  // lets be explicit about module type
  outExtension({ format }) {
    if (format === "cjs") {
      return { js: ".cjs" };
    } else if (format === "esm") {
      return { js: ".mjs" };
    }
    throw new Error(`Format ${format} is not supported`);
  },
  clean: false,
  dts: true,
  sourcemap: true,
  splitting: false,
  treeshake: true,
  format: ["cjs", "esm"],
  silent: true
});

export { getDefaultConfig };
//# sourceMappingURL=index.mjs.map
//# sourceMappingURL=index.mjs.map