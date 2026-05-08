import fs from 'node:fs';
import path from 'node:path';

import * as yaml from 'js-yaml';

import type { LlmProvider } from '../types/llm';

export type EvalCase = {
	id: string;
	question: string;
	expected_sql_contains?: string[];
	forbidden_sql_contains?: string[];
	expected_columns?: string[];
	expected_rows?: Array<Record<string, unknown>>;
	threshold?: number;
	file?: string;
};

export type EvalToolCall = {
	toolName?: string;
	input?: Record<string, unknown>;
	output?: Record<string, unknown>;
};

export type EvalRunResult = {
	id: string;
	passed: boolean;
	model: string;
	durationMs: number;
	checks: {
		sqlContains?: { passed: boolean; missing: string[] };
		sqlForbidden?: { passed: boolean; found: string[] };
		rows?: { passed: boolean; message: string; diff?: string };
	};
	message: string;
	generatedSql: string | null;
	responseText: string;
	actualRows: Array<Record<string, unknown>>;
	expectedRows: Array<Record<string, unknown>>;
	toolCalls: EvalToolCall[];
	timestamp: string;
	error?: string;
};

export function loadEvalCases(projectDir: string): EvalCase[] {
	const testsDir = path.join(projectDir, 'tests');
	if (!fs.existsSync(testsDir)) {
		return [];
	}

	const cases: EvalCase[] = [];
	const seenIds = new Set<string>();
	const files = fs.readdirSync(testsDir)
		.filter(file => (file.endsWith('.yml') || file.endsWith('.yaml')) && !file.startsWith('.'))
		.sort();

	for (const file of files) {
		const parsed = yaml.load(fs.readFileSync(path.join(testsDir, file), 'utf8'));
		const items = Array.isArray(parsed) ? parsed : [parsed];
		for (const [index, item] of items.entries()) {
			if (!item || typeof item !== 'object' || Array.isArray(item)) {
				throw new Error(`${file} case #${index + 1} must be a mapping`);
			}
			const data = item as Record<string, unknown>;
			const id = stringField(data.id);
			const question = stringField(data.question) ?? stringField(data.prompt);
			if (!id) {
				throw new Error(`${file} case #${index + 1} must define id`);
			}
			if (seenIds.has(id)) {
				throw new Error(`Duplicate eval id: ${id}`);
			}
			if (!question) {
				throw new Error(`${file} case ${id} must define question`);
			}

			seenIds.add(id);
			cases.push({
				id,
				question,
				expected_sql_contains: stringArray(data.expected_sql_contains),
				forbidden_sql_contains: stringArray(data.forbidden_sql_contains),
				expected_columns: stringArray(data.expected_columns),
				expected_rows: rowArray(data.expected_rows),
				threshold: typeof data.threshold === 'number' ? data.threshold : undefined,
				file,
			});
		}
	}

	return cases;
}

export function listLatestEvalResults(projectDir: string) {
	const outputsDir = path.join(projectDir, 'tests', 'outputs');
	const latest = new Map<string, Pick<EvalRunResult, 'id' | 'passed' | 'model' | 'durationMs' | 'message' | 'timestamp'>>();
	if (!fs.existsSync(outputsDir)) {
		return latest;
	}

	const files = fs.readdirSync(outputsDir)
		.filter(file => file.startsWith('results_') && file.endsWith('.json'))
		.sort()
		.reverse();

	for (const file of files) {
		try {
			const parsed = JSON.parse(fs.readFileSync(path.join(outputsDir, file), 'utf8'));
			const results = Array.isArray(parsed.results) ? parsed.results : [];
			for (const result of results) {
				if (!result?.id || latest.has(result.id)) {
					continue;
				}
				const normalized = normalizeStoredResult(result);
				const durationMs = typeof result.durationMs === 'number' ? result.durationMs : result.duration_ms;
				latest.set(result.id, {
					id: result.id,
					passed: normalized.passed,
					model: result.model,
					durationMs,
					message: normalized.message,
					timestamp: result.timestamp ?? parsed.timestamp,
				});
			}
		} catch (error) {
			console.error(`Failed to read eval result ${file}:`, error);
		}
	}

	return latest;
}

function normalizeStoredResult(result: Record<string, unknown>) {
	const toolCalls: unknown[] = Array.isArray(result.toolCalls)
		? result.toolCalls
		: typeof result.details === 'object' && result.details && Array.isArray((result.details as Record<string, unknown>).tool_calls)
			? ((result.details as Record<string, unknown>).tool_calls as unknown[])
			: [];
	const sqlTool = [...toolCalls].reverse().find((call: any) => call?.toolName === 'execute_bigquery_sql') as EvalToolCall | undefined;
	const sqlError = typeof sqlTool?.output?.error === 'string' ? sqlTool.output.error : undefined;
	if (sqlError) {
		return {
			passed: false,
			message: `SQL execution failed: ${sqlError}`,
		};
	}
	return {
		passed: !!result.passed,
		message: typeof result.message === 'string' ? result.message : '',
	};
}

export function verifyEvalCase(params: {
	testCase: EvalCase;
	toolCalls: EvalToolCall[];
	responseText: string;
	model: string;
	durationMs: number;
	timestamp?: string;
}): EvalRunResult {
	const { testCase, toolCalls, responseText, model, durationMs } = params;
	const timestamp = params.timestamp ?? new Date().toISOString();
	const sqlTool = [...toolCalls].reverse().find(call => call.toolName === 'execute_bigquery_sql');
	const generatedSql = extractSql(sqlTool);
	const actualRows = extractRows(sqlTool);
	const sqlError = typeof sqlTool?.output?.error === 'string' ? sqlTool.output.error : undefined;
	const expectedRows = testCase.expected_rows ?? [];
	const checks: EvalRunResult['checks'] = {};
	const failures: string[] = [];
	let assertionCount = 0;

	if (testCase.expected_sql_contains?.length) {
		assertionCount++;
		const sql = generatedSql?.toLowerCase() ?? '';
		const missing = testCase.expected_sql_contains.filter(fragment => !sql.includes(fragment.toLowerCase()));
		checks.sqlContains = { passed: missing.length === 0, missing };
		if (missing.length) {
			failures.push(`Missing SQL fragments: ${missing.join(', ')}`);
		}
	}

	if (testCase.forbidden_sql_contains?.length) {
		assertionCount++;
		const sql = generatedSql?.toLowerCase() ?? '';
		const found = testCase.forbidden_sql_contains.filter(fragment => sql.includes(fragment.toLowerCase()));
		checks.sqlForbidden = { passed: found.length === 0, found };
		if (found.length) {
			failures.push(`Forbidden SQL fragments found: ${found.join(', ')}`);
		}
	}

	if (expectedRows.length) {
		assertionCount++;
		const rowCheck = compareRows(actualRows, expectedRows, testCase.expected_columns);
		checks.rows = rowCheck;
		if (!rowCheck.passed) {
			failures.push(rowCheck.message);
		}
	}

	if (assertionCount === 0) {
		failures.push('No assertions configured');
	}
	if (!generatedSql && (testCase.expected_sql_contains?.length || testCase.forbidden_sql_contains?.length || expectedRows.length)) {
		failures.push('No execute_bigquery_sql tool result found');
	}
	if (sqlError) {
		failures.push(`SQL execution failed: ${sqlError}`);
	}

	return {
		id: testCase.id,
		passed: failures.length === 0,
		model,
		durationMs,
		checks,
		message: failures.length ? failures.join('; ') : 'pass',
		generatedSql,
		responseText,
		actualRows,
		expectedRows,
		toolCalls,
		timestamp,
		error: sqlError,
	};
}

export function saveEvalResult(projectDir: string, result: EvalRunResult) {
	const outputsDir = path.join(projectDir, 'tests', 'outputs');
	fs.mkdirSync(outputsDir, { recursive: true });
	const timestamp = result.timestamp.replace(/[-:]/g, '').replace(/\.\d{3}Z$/, 'Z');
	const outputPath = path.join(outputsDir, `results_${timestamp}.json`);
	const data = {
		timestamp: result.timestamp,
		results: [result],
		summary: {
			total: 1,
			passed: result.passed ? 1 : 0,
			failed: result.passed ? 0 : 1,
			total_duration_ms: result.durationMs,
		},
	};
	fs.writeFileSync(outputPath, JSON.stringify(data, null, 2));
	return outputPath;
}

export function modelLabel(provider: LlmProvider, modelId: string) {
	return `${provider}:${modelId}`;
}

function stringField(value: unknown) {
	return typeof value === 'string' && value.trim() ? value.trim() : undefined;
}

function stringArray(value: unknown) {
	return Array.isArray(value) ? value.filter((item): item is string => typeof item === 'string') : undefined;
}

function rowArray(value: unknown) {
	if (!Array.isArray(value)) {
		return undefined;
	}
	return value.filter((item): item is Record<string, unknown> => !!item && typeof item === 'object' && !Array.isArray(item));
}

function extractSql(call: EvalToolCall | undefined) {
	const outputSql = call?.output?.sql;
	const inputSql = call?.input?.sql;
	return typeof outputSql === 'string' ? outputSql : typeof inputSql === 'string' ? inputSql : null;
}

function extractRows(call: EvalToolCall | undefined) {
	const rows = call?.output?.results ?? call?.output?.rows;
	return Array.isArray(rows) ? rows.filter((row): row is Record<string, unknown> => !!row && typeof row === 'object' && !Array.isArray(row)) : [];
}

function compareRows(
	actualRows: Array<Record<string, unknown>>,
	expectedRows: Array<Record<string, unknown>>,
	expectedColumns?: string[],
) {
	if (!actualRows.length) {
		return { passed: false, message: 'actual is empty' };
	}
	const columns = expectedColumns?.length ? expectedColumns : Array.from(new Set(expectedRows.flatMap(row => Object.keys(row))));
	const missingColumns = columns.filter(column => actualRows.some(row => !(column in row)));
	if (missingColumns.length) {
		return { passed: false, message: `missing columns: ${missingColumns.join(', ')}` };
	}
	if (actualRows.length !== expectedRows.length) {
		return { passed: false, message: `row count: ${actualRows.length} vs ${expectedRows.length}` };
	}

	const normalize = (rows: Array<Record<string, unknown>>) => rows
		.map(row => Object.fromEntries(columns.map(column => [column, row[column]])))
		.sort((a, b) => JSON.stringify(a).localeCompare(JSON.stringify(b)));
	const actual = normalize(actualRows);
	const expected = normalize(expectedRows);

	for (let rowIndex = 0; rowIndex < expected.length; rowIndex++) {
		for (const column of columns) {
			if (!valuesEqual(actual[rowIndex][column], expected[rowIndex][column])) {
				return {
					passed: false,
					message: `values differ at row ${rowIndex + 1}, column ${column}`,
					diff: `actual=${String(actual[rowIndex][column])}; expected=${String(expected[rowIndex][column])}`,
				};
			}
		}
	}

	return { passed: true, message: 'match' };
}

function valuesEqual(actual: unknown, expected: unknown) {
	if (typeof actual === 'number' && typeof expected === 'number') {
		return Math.abs(actual - expected) <= 1e-8 + 1e-5 * Math.abs(expected);
	}
	return String(actual) === String(expected);
}
