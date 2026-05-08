import fastify from 'fastify';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

let projectDir: string;

async function buildCoreApp() {
	vi.resetModules();
	process.env.CA3_DEFAULT_PROJECT_PATH = projectDir;
	const { coreRoutes } = await import('../src/routes/core');
	const app = fastify();
	await app.register(coreRoutes, { prefix: '/api/core' });
	return app;
}

function writeFile(relPath: string, content: string) {
	const filePath = path.join(projectDir, relPath);
	fs.mkdirSync(path.dirname(filePath), { recursive: true });
	fs.writeFileSync(filePath, content);
}

describe('core routes', () => {
	beforeEach(() => {
		projectDir = fs.mkdtempSync(path.join(os.tmpdir(), 'ca3-core-routes-'));
	});

	afterEach(() => {
		fs.rmSync(projectDir, { recursive: true, force: true });
		delete process.env.CA3_DEFAULT_PROJECT_PATH;
	});

	it('blocks path traversal in table metadata route', async () => {
		const app = await buildCoreApp();

		const response = await app.inject({
			method: 'GET',
			url: '/api/core/tables/..__DOT__..__DOT__etc__DOT__passwd',
		});

		expect(response.statusCode).toBe(403);
		expect(response.json()).toMatchObject({
			status: 'error',
			message: 'Forbidden: Path traversal detected',
		});
		await app.close();
	});

	it('resolves deep encoded table FQDNs to metadata files', async () => {
		const tableRel = 'databases/type=bigquery/database=demo/schema=analytics/table=events';
		writeFile(`${tableRel}/columns.md`, '# Columns');
		writeFile(`${tableRel}/preview.md`, '# Preview');
		writeFile(`${tableRel}/profiling.md`, '# Profiling');
		writeFile(`${tableRel}/how_to_use.md`, '# Usage');

		const app = await buildCoreApp();
		const response = await app.inject({
			method: 'GET',
			url: '/api/core/tables/type=bigquery__DOT__database=demo__DOT__schema=analytics__DOT__table=events',
		});

		expect(response.statusCode).toBe(200);
		expect(response.json()).toEqual({
			status: 'ok',
			table: {
				fqdn: 'type=bigquery__DOT__database=demo__DOT__schema=analytics__DOT__table=events',
				columns: '# Columns',
				preview: '# Preview',
				profiling: '# Profiling',
				howToUse: '# Usage',
			},
		});
		await app.close();
	});

	it('loads eval YAML files dynamically and ignores dotfiles', async () => {
		writeFile('tests/bigquery_suite.yml', '- id: visible_case\n  question: How many rows?\n');
		writeFile('tests/.hidden.yml', '- id: hidden_case\n  question: hidden\n');
		writeFile('tests/._bigquery_suite.yml', 'not: valid');

		const app = await buildCoreApp();
		const first = await app.inject({ method: 'GET', url: '/api/core/evals' });

		expect(first.statusCode).toBe(200);
		expect(first.json().evals).toEqual([
			{ id: 'visible_case', question: 'How many rows?', file: 'bigquery_suite.yml', lastResult: null },
		]);

		writeFile('tests/new_suite.yaml', '- id: new_case\n  question: What changed?\n');
		const second = await app.inject({ method: 'GET', url: '/api/core/evals' });

		expect(second.json().evals.map((item: any) => item.id).sort()).toEqual(['new_case', 'visible_case']);
		await app.close();
	});

	it('includes latest eval result summaries', async () => {
		writeFile('tests/bigquery_suite.yml', '- id: visible_case\n  question: How many rows?\n');
		writeFile('tests/outputs/results_20260508T010000Z.json', JSON.stringify({
			timestamp: '2026-05-08T01:00:00.000Z',
			results: [
				{
					id: 'visible_case',
					passed: true,
					model: 'anthropic:claude-haiku-4-5-20251001',
					durationMs: 25,
					message: 'pass',
					timestamp: '2026-05-08T01:00:00.000Z',
				},
			],
		}));

		const app = await buildCoreApp();
		const response = await app.inject({ method: 'GET', url: '/api/core/evals' });

		expect(response.json().evals[0].lastResult).toMatchObject({
			id: 'visible_case',
			passed: true,
			model: 'anthropic:claude-haiku-4-5-20251001',
			message: 'pass',
		});
		await app.close();
	});

	it('runs a single eval case through AgentManager and saves JSON history', async () => {
		writeFile('ca3_config.yaml', [
			'project_name: eval-test',
			'llm:',
			'  provider: anthropic',
			'  annotation_model: claude-haiku-4-5-20251001',
			'  api_key: config-key',
		].join('\n'));
		writeFile('tests/bigquery_suite.yml', [
			'- id: tech_keywords_count',
			'  question: How many tech keywords?',
			'  expected_sql_contains: ["COUNT", "tech_keywords"]',
			'  expected_columns: ["count"]',
			'  expected_rows:',
			'    - count: 42',
		].join('\n'));
		vi.doMock('../src/lib/agents/agent-manager', () => ({
			AgentManager: class {
				async streamResponse(_prompt: string, onProgress: (event: any) => void) {
					onProgress({
						type: 'tool_result',
						toolName: 'execute_bigquery_sql',
						input: { sql: 'SELECT COUNT(*) AS count FROM tech_keywords' },
						output: { results: [{ count: 42 }], columns: ['count'], rowCount: 1 },
					});
					onProgress({ type: 'message_delta', content: 'There are 42.' });
				}
			},
		}));

		const app = await buildCoreApp();
		const response = await app.inject({
			method: 'POST',
			url: '/api/core/evals/run',
			payload: { id: 'tech_keywords_count' },
		});

		expect(response.statusCode).toBe(200);
		expect(response.json().result).toMatchObject({
			id: 'tech_keywords_count',
			passed: true,
			model: 'anthropic:claude-haiku-4-5-20251001',
			generatedSql: 'SELECT COUNT(*) AS count FROM tech_keywords',
			actualRows: [{ count: 42 }],
			expectedRows: [{ count: 42 }],
		});
		expect(fs.readdirSync(path.join(projectDir, 'tests', 'outputs')).some(file => file.startsWith('results_'))).toBe(true);
		await app.close();
	});

	it('returns 404 for an unknown eval id', async () => {
		writeFile('tests/bigquery_suite.yml', '- id: visible_case\n  question: How many rows?\n');
		const app = await buildCoreApp();

		const response = await app.inject({
			method: 'POST',
			url: '/api/core/evals/run',
			payload: { id: 'missing_case' },
		});

		expect(response.statusCode).toBe(404);
		expect(response.json()).toMatchObject({ status: 'error', message: 'Evaluation not found: missing_case' });
		await app.close();
	});
});
