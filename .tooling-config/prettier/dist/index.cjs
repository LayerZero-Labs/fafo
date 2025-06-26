'use strict';

var packageJson = require('prettier-plugin-packagejson');
var sh = require('prettier-plugin-sh');
var toml = require('prettier-plugin-toml');

function _interopDefault (e) { return e && e.__esModule ? e : { default: e }; }

var packageJson__default = /*#__PURE__*/_interopDefault(packageJson);
var sh__default = /*#__PURE__*/_interopDefault(sh);
var toml__default = /*#__PURE__*/_interopDefault(toml);

// src/index.ts
var config = {
  // Default settings (will be overridden by specific overrides)
  printWidth: 120,
  tabWidth: 4,
  useTabs: false,
  semi: true,
  singleQuote: true,
  quoteProps: "as-needed",
  trailingComma: "all",
  bracketSpacing: true,
  bracketSameLine: false,
  arrowParens: "always",
  plugins: [packageJson__default.default, sh__default.default, toml__default.default],
  // File-specific configurations
  overrides: [
    {
      // JavaScript
      files: ["*.js", "*.jsx", "*.mjs", "*.cjs"],
      options: {
        parser: "babel",
        semi: true,
        printWidth: 120,
        tabWidth: 4,
        trailingComma: "all",
        bracketSpacing: true,
        bracketSameLine: false,
        arrowParens: "always"
      }
    },
    {
      // TypeScript
      files: ["*.ts", "*.tsx", "*.mts", "*.cts"],
      options: {
        parser: "typescript",
        semi: false,
        // TypeScript community often prefers no semicolons
        printWidth: 120,
        tabWidth: 4,
        trailingComma: "all",
        bracketSpacing: true,
        bracketSameLine: false,
        arrowParens: "always"
      }
    },
    {
      // JSON files
      files: ["*.json"],
      options: {
        parser: "json",
        printWidth: 120,
        tabWidth: 4
      }
    },
    {
      // Package.json specific
      files: ["package.json"],
      options: {
        parser: "json-stringify",
        printWidth: 120,
        tabWidth: 2
      }
    },
    {
      // Markdown
      files: ["*.md", "*.mdx"],
      options: {
        parser: "markdown",
        printWidth: 120,
        tabWidth: 4,
        proseWrap: "always",
        embeddedLanguageFormatting: "auto"
      }
    },
    {
      // YAML
      files: ["*.yml", "*.yaml"],
      options: {
        parser: "yaml",
        printWidth: 120,
        tabWidth: 2,
        singleQuote: true,
        trailingComma: "none"
      }
    },
    {
      // TOML
      files: ["*.toml"],
      options: {
        parser: "toml",
        printWidth: 120,
        tabWidth: 4,
        singleQuote: true
      }
    },
    {
      // Shell scripts
      files: ["*.sh", ".bashrc", ".zshrc", ".env*"],
      options: {
        parser: "sh",
        printWidth: 120,
        tabWidth: 4,
        // Shell script specific options
        indent: 4,
        keepComments: true,
        switchCaseIndent: true
      }
    }
  ]
};
var index_default = config;

module.exports = index_default;
//# sourceMappingURL=index.cjs.map
//# sourceMappingURL=index.cjs.map