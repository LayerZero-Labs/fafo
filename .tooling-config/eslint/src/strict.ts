import * as tseslint from 'typescript-eslint'

import recommended from './recommended'
// import type { TSESLint } from '@typescript-eslint/utils'
// https://github.com/typescript-eslint/typescript-eslint/issues/8613#issuecomment-1983584487
// use @typescript-eslint/utils instead of @types/eslint to resolve the incompatible issue
// const config: TSESLint.FlatConfig.Config[] = [
const config = tseslint.config(...recommended, {
    rules: {
        // Strict TypeScript rules
        '@typescript-eslint/explicit-function-return-type': 'error',
        '@typescript-eslint/explicit-module-boundary-types': 'error',
        '@typescript-eslint/no-explicit-any': 'error',
        '@typescript-eslint/strict-boolean-expressions': 'error',
        '@typescript-eslint/no-unnecessary-condition': 'error',
        '@typescript-eslint/no-floating-promises': 'error',
        '@typescript-eslint/no-misused-promises': 'error',
        '@typescript-eslint/await-thenable': 'error',
        '@typescript-eslint/no-for-in-array': 'error',
        '@typescript-eslint/no-unsafe-assignment': 'error',
        '@typescript-eslint/no-unsafe-call': 'error',
        '@typescript-eslint/no-unsafe-member-access': 'error',
        '@typescript-eslint/no-unsafe-return': 'error',

        // Additional strict rules
        'no-console': 'error',
        'no-debugger': 'error',
        'no-alert': 'error',
        'no-var': 'error',
        'prefer-const': 'error',
        eqeqeq: ['error', 'always'],
    },
})

export default config
