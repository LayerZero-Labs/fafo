import { fileURLToPath } from 'url';
import eslint from '@eslint/js';
import jsonPlugin from '@eslint/json';
import markdownPlugin from '@eslint/markdown';
import { createTypeScriptImportResolver } from 'eslint-import-resolver-typescript';
import eslintPluginImportX from 'eslint-plugin-import-x';
import pluginJest from 'eslint-plugin-jest';
import prettierPlugin from 'eslint-plugin-prettier/recommended';
import globals from 'globals';
import * as tseslint from 'typescript-eslint';

// src/recommended.ts
var typescriptConfig = {
  ...eslintPluginImportX.flatConfigs.typescript,
  settings: {
    "import-x/parsers": {
      // Use import.meta.resolve to get the absolute path to @typescript-eslint/parser
      // This is required for shared config to properly resolve parser
      [fileURLToPath(import.meta.resolve("@typescript-eslint/parser"))]: [".ts", ".tsx"]
    },
    // The legacy resolver interface is not supported by eslint-import-resolver-typescript,
    // so we must use import-x/resolver-next instead of import-x/resolver.
    // Using import-x/resolver would throw: "typescript with invalid interface loaded as resolver"
    // 'import-x/resolver': {
    //     typescript: true,
    // }
    "import-x/resolver-next": [
      createTypeScriptImportResolver({
        alwaysTryTypes: true
      })
    ]
  }
};
var config2 = tseslint.config(
  eslint.configs.recommended,
  jsonPlugin.configs.recommended,
  markdownPlugin.configs.recommended,
  // Shared rules for all JavaScript/TypeScript files
  {
    files: ["**/*.{js,jsx,mjs,cjs,ts,tsx,mts,cts}"],
    settings: {
      "import-x/internal-regex": "^@layerresearch/"
    },
    extends: [eslintPluginImportX.flatConfigs.recommended],
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
    extends: [tseslint.configs.recommended, typescriptConfig],
    plugins: {
      "typescript-eslint": tseslint.plugin,
      "@typescript-eslint": tseslint.plugin
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
        ...globals.node
      }
    }
  },
  // Jest-specific rules
  {
    files: ["**/*.spec.js", "**/*.test.js"],
    plugins: { jest: pluginJest },
    languageOptions: {
      globals: {
        ...pluginJest.environments.globals.globals
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
  prettierPlugin
);
var recommended_default = config2;
var config4 = tseslint.config(...recommended_default, {
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

// src/index.ts
var index_default = {
  recommended: recommended_default,
  strict: strict_default
};

export { index_default as default };
//# sourceMappingURL=index.mjs.map
//# sourceMappingURL=index.mjs.map