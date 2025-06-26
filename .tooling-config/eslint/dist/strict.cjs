'use strict';

var tseslint = require('typescript-eslint');
var url = require('url');
var eslint = require('@eslint/js');
var jsonPlugin = require('@eslint/json');
var markdownPlugin = require('@eslint/markdown');
var eslintImportResolverTypescript = require('eslint-import-resolver-typescript');
var eslintPluginImportX = require('eslint-plugin-import-x');
var pluginJest = require('eslint-plugin-jest');
var prettierPlugin = require('eslint-plugin-prettier/recommended');
var globals = require('globals');

function _interopDefault (e) { return e && e.__esModule ? e : { default: e }; }

function _interopNamespace(e) {
  if (e && e.__esModule) return e;
  var n = Object.create(null);
  if (e) {
    Object.keys(e).forEach(function (k) {
      if (k !== 'default') {
        var d = Object.getOwnPropertyDescriptor(e, k);
        Object.defineProperty(n, k, d.get ? d : {
          enumerable: true,
          get: function () { return e[k]; }
        });
      }
    });
  }
  n.default = e;
  return Object.freeze(n);
}

var tseslint__namespace = /*#__PURE__*/_interopNamespace(tseslint);
var eslint__default = /*#__PURE__*/_interopDefault(eslint);
var jsonPlugin__default = /*#__PURE__*/_interopDefault(jsonPlugin);
var markdownPlugin__default = /*#__PURE__*/_interopDefault(markdownPlugin);
var eslintPluginImportX__default = /*#__PURE__*/_interopDefault(eslintPluginImportX);
var pluginJest__default = /*#__PURE__*/_interopDefault(pluginJest);
var prettierPlugin__default = /*#__PURE__*/_interopDefault(prettierPlugin);
var globals__default = /*#__PURE__*/_interopDefault(globals);

// src/strict.ts
var typescriptConfig = {
  ...eslintPluginImportX__default.default.flatConfigs.typescript,
  settings: {
    "import-x/parsers": {
      // Use import.meta.resolve to get the absolute path to @typescript-eslint/parser
      // This is required for shared config to properly resolve parser
      [url.fileURLToPath(undefined("@typescript-eslint/parser"))]: [".ts", ".tsx"]
    },
    // The legacy resolver interface is not supported by eslint-import-resolver-typescript,
    // so we must use import-x/resolver-next instead of import-x/resolver.
    // Using import-x/resolver would throw: "typescript with invalid interface loaded as resolver"
    // 'import-x/resolver': {
    //     typescript: true,
    // }
    "import-x/resolver-next": [
      eslintImportResolverTypescript.createTypeScriptImportResolver({
        alwaysTryTypes: true
      })
    ]
  }
};
var config2 = tseslint__namespace.config(
  eslint__default.default.configs.recommended,
  jsonPlugin__default.default.configs.recommended,
  markdownPlugin__default.default.configs.recommended,
  // Shared rules for all JavaScript/TypeScript files
  {
    files: ["**/*.{js,jsx,mjs,cjs,ts,tsx,mts,cts}"],
    settings: {
      "import-x/internal-regex": "^@layerresearch/"
    },
    extends: [eslintPluginImportX__default.default.flatConfigs.recommended],
    rules: {
      "import-x/order": [
        "error",
        {
          groups: ["builtin", "external", "internal", ["parent", "sibling"], "index", "object", "type"],
          "newlines-between": "always",
          alphabetize: {
            order: "asc",
            caseInsensitive: true
          }
        }
      ],
      "import-x/no-unresolved": "error",
      "import-x/no-cycle": "error",
      "import-x/no-self-import": "error"
    }
  },
  // TypeScript-specific rules
  {
    files: ["**/*.{ts,tsx,mts,cts}"],
    extends: [tseslint__namespace.configs.recommended, typescriptConfig],
    plugins: {
      "typescript-eslint": tseslint__namespace.plugin,
      "@typescript-eslint": tseslint__namespace.plugin
    },
    rules: {
      "@typescript-eslint/no-unused-vars": ["warn", { argsIgnorePattern: "^_" }],
      "@typescript-eslint/explicit-function-return-type": "off",
      "@typescript-eslint/explicit-module-boundary-types": "off",
      "@typescript-eslint/no-explicit-any": "warn"
    }
  },
  // JavaScript-specific rules
  {
    files: ["**/*.js", "**/*.jsx", "**/*.mjs", "**/*.cjs"],
    languageOptions: {
      ecmaVersion: 2022,
      sourceType: "module",
      globals: {
        ...globals__default.default.node
      }
    }
  },
  // Jest-specific rules
  {
    files: ["**/*.spec.js", "**/*.test.js"],
    plugins: { jest: pluginJest__default.default },
    languageOptions: {
      globals: {
        ...pluginJest__default.default.environments.globals.globals
      }
    },
    rules: {
      "jest/no-disabled-tests": "warn",
      "jest/no-focused-tests": "error",
      "jest/no-identical-title": "error",
      "jest/prefer-to-have-length": "warn",
      "jest/valid-expect": "error"
    }
  },
  prettierPlugin__default.default
);
var recommended_default = config2;

// src/strict.ts
var config4 = tseslint__namespace.config(...recommended_default, {
  rules: {
    // Strict TypeScript rules
    "@typescript-eslint/explicit-function-return-type": "error",
    "@typescript-eslint/explicit-module-boundary-types": "error",
    "@typescript-eslint/no-explicit-any": "error",
    "@typescript-eslint/strict-boolean-expressions": "error",
    "@typescript-eslint/no-unnecessary-condition": "error",
    "@typescript-eslint/no-floating-promises": "error",
    "@typescript-eslint/no-misused-promises": "error",
    "@typescript-eslint/await-thenable": "error",
    "@typescript-eslint/no-for-in-array": "error",
    "@typescript-eslint/no-unsafe-assignment": "error",
    "@typescript-eslint/no-unsafe-call": "error",
    "@typescript-eslint/no-unsafe-member-access": "error",
    "@typescript-eslint/no-unsafe-return": "error",
    // Additional strict rules
    "no-console": "error",
    "no-debugger": "error",
    "no-alert": "error",
    "no-var": "error",
    "prefer-const": "error",
    eqeqeq: ["error", "always"]
  }
});
var strict_default = config4;

module.exports = strict_default;
//# sourceMappingURL=strict.cjs.map
//# sourceMappingURL=strict.cjs.map