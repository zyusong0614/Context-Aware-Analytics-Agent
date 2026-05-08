import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';

import { afterEach, beforeEach, describe, expect, it } from 'vitest';

let projectDir: string;

describe('project config', () => {
	beforeEach(() => {
		projectDir = fs.mkdtempSync(path.join(os.tmpdir(), 'ca3-project-config-'));
	});

	afterEach(() => {
		fs.rmSync(projectDir, { recursive: true, force: true });
	});

	it('resolves provider and model from ca3_config.yaml', async () => {
		fs.writeFileSync(
			path.join(projectDir, 'ca3_config.yaml'),
			[
				'project_name: demo',
				'llm:',
				'  provider: openai',
				'  model_id: gpt-5.2',
				'  api_key: config-key',
			].join('\n'),
		);

		const { resolveProjectLlmConfig } = await import('../src/lib/project-config');

		expect(resolveProjectLlmConfig(projectDir)).toEqual({
			provider: 'openai',
			modelId: 'gpt-5.2',
			apiKey: 'config-key',
		});
	});

	it('supports legacy annotation_model and explicit runtime overrides', async () => {
		fs.writeFileSync(
			path.join(projectDir, 'ca3_config.yaml'),
			[
				'llm:',
				'  provider: anthropic',
				'  annotation_model: claude-haiku-4-5-20251001',
			].join('\n'),
		);

		const { resolveProjectLlmConfig } = await import('../src/lib/project-config');

		expect(resolveProjectLlmConfig(projectDir, { provider: 'google', modelId: 'gemini-2.5-flash' })).toMatchObject({
			provider: 'google',
			modelId: 'gemini-2.5-flash',
		});
	});
});
