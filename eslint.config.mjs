import eslintConfig from '@tooling/eslint-config';

export default [
    {
        ignores: [
            // Build outputs and dependencies
            '**/dist/**',
            '**/build/**',
            '**/node_modules/**',
            '**/coverage/**',
            '**/.turbo/**',
            '**/.next/**',
            '**/.cache/**',

            // Generated files
            '**/*.generated.*',
            '**/*.gen.*',

            // Non JavaScript/TypeScript files
            '**/*.css',
            '**/*.scss',
            '**/*.html',
            '**/*.json',
            '**/*.yaml',
            '**/*.yml',
            '**/*.md',
            '**/*.mdx',
            '**/*.toml',
            '**/*.sh',
            '**/*.env*',

            // Rust specific
            '**/target/**',
            '**/Cargo.lock',
        ],
    },
    ...eslintConfig.recommended,
];
