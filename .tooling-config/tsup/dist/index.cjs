'use strict';

var esbuildNodeExternals = require('esbuild-node-externals');
var tsup = require('tsup');

// src/index.ts
var getDefaultConfig = () => ({
  esbuildPlugins: [
    esbuildNodeExternals.nodeExternalsPlugin({
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

Object.defineProperty(exports, "defineConfig", {
  enumerable: true,
  get: function () { return tsup.defineConfig; }
});
exports.getDefaultConfig = getDefaultConfig;
//# sourceMappingURL=index.cjs.map
//# sourceMappingURL=index.cjs.map