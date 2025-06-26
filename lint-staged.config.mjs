// @ts-check
/** @type {import('lint-staged').Configuration} */
const config = {
    '*.{js,cjs,mjs,jsx,ts,cts,mts,tsx}': ['eslint --fix', 'prettier --write'],
    '*.{md,mdx}': ['prettier --write'],
    '*.{json,toml,yaml,yml}': ['prettier --write'],
    '*.rs': (files) => ['cargo clippy --workspace --allow-staged --allow-dirty --fix', `rustfmt ${files.join(' ')}`],
};

export default config;
