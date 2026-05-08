import js from './apps/backend/node_modules/@eslint/js/src/index.js';
import tseslint from './apps/backend/node_modules/typescript-eslint/dist/index.js';

export default [
	{
		ignores: [
			'**/node_modules/**',
			'**/dist/**',
			'**/.svelte-kit/**',
			'**/migrations*/**',
			'**/._*',
			'**/.!*',
		],
	},
	js.configs.recommended,
	...tseslint.configs.recommended.map((config) => ({
		...config,
		rules: {
			...config.rules,
			'@typescript-eslint/no-explicit-any': 'off',
			'@typescript-eslint/no-unused-vars': ['warn', { argsIgnorePattern: '^_' }],
		},
	})),
];
