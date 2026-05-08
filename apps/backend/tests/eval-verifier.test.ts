import { describe, expect, it } from 'vitest';

import { verifyEvalCase } from '../src/lib/evals';

describe('eval verifier', () => {
	it('passes SQL contains checks and row comparison ignoring row order', () => {
		const result = verifyEvalCase({
			testCase: {
				id: 'companies',
				question: 'List companies',
				expected_sql_contains: ['SELECT', 'company_name'],
				forbidden_sql_contains: ['DROP'],
				expected_columns: ['company_name'],
				expected_rows: [{ company_name: 'B' }, { company_name: 'A' }],
			},
			toolCalls: [
				{
					toolName: 'execute_bigquery_sql',
					input: { sql: 'SELECT company_name FROM companies' },
					output: {
						results: [{ company_name: 'A', extra: 1 }, { company_name: 'B', extra: 2 }],
					},
				},
			],
			responseText: 'A and B',
			model: 'anthropic:test',
			durationMs: 10,
			timestamp: '2026-05-08T00:00:00.000Z',
		});

		expect(result.passed).toBe(true);
		expect(result.checks.rows).toEqual({ passed: true, message: 'match' });
	});

	it('fails clearly when assertions are missing', () => {
		const result = verifyEvalCase({
			testCase: { id: 'empty', question: 'No checks' },
			toolCalls: [],
			responseText: '',
			model: 'anthropic:test',
			durationMs: 10,
		});

		expect(result.passed).toBe(false);
		expect(result.message).toBe('No assertions configured');
	});

	it('reports missing columns and forbidden SQL fragments', () => {
		const result = verifyEvalCase({
			testCase: {
				id: 'bad',
				question: 'Bad SQL',
				forbidden_sql_contains: ['DROP'],
				expected_columns: ['count'],
				expected_rows: [{ count: 1 }],
			},
			toolCalls: [
				{
					toolName: 'execute_bigquery_sql',
					input: { sql: 'SELECT name FROM t; DROP TABLE t' },
					output: { results: [{ name: 'x' }] },
				},
			],
			responseText: '',
			model: 'anthropic:test',
			durationMs: 10,
		});

		expect(result.passed).toBe(false);
		expect(result.checks.sqlForbidden).toEqual({ passed: false, found: ['DROP'] });
		expect(result.checks.rows).toMatchObject({ passed: false, message: 'missing columns: count' });
	});

	it('fails when the SQL tool returns an execution error even if SQL text matches', () => {
		const result = verifyEvalCase({
			testCase: {
				id: 'sql_error',
				question: 'Count rows',
				expected_sql_contains: ['COUNT'],
			},
			toolCalls: [
				{
					toolName: 'execute_bigquery_sql',
					input: { sql: 'SELECT COUNT(*) FROM t' },
					output: { error: 'Connection to SQL engine failed' },
				},
			],
			responseText: '',
			model: 'anthropic:test',
			durationMs: 10,
		});

		expect(result.passed).toBe(false);
		expect(result.error).toBe('Connection to SQL engine failed');
		expect(result.message).toContain('SQL execution failed');
	});
});
