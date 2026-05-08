import axios from 'axios';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import { createSqlTools } from '../src/lib/tools/sql';

vi.mock('axios');

const postMock = vi.mocked(axios.post);

beforeEach(() => {
	postMock.mockReset();
});

function executeSql(sql: string) {
	const tools = createSqlTools('http://sidecar.test', '/tmp/ca3-project');
	return (tools.execute_bigquery_sql.execute as any)({ sql }, {});
}

describe('execute_bigquery_sql safety', () => {
	it('injects LIMIT 100 when a SELECT query has no limit', async () => {
		postMock.mockResolvedValueOnce({
			data: { data: [{ count: 1 }], row_count: 1, columns: ['count'] },
		});

		const result = await executeSql('SELECT count(*) AS count FROM dataset.table');

		expect(postMock).toHaveBeenCalledWith('http://sidecar.test/execute_sql', {
			sql: 'SELECT count(*) AS count FROM dataset.table LIMIT 100',
			ca3_project_folder: '/tmp/ca3-project',
		});
		expect(result).toMatchObject({ rowCount: 1, message: 'Success' });
	});

	it('blocks placeholder project and dataset IDs before execution', async () => {
		await expect(executeSql('SELECT * FROM project_id.dataset.table')).resolves.toEqual({
			error: 'Placeholder detected (project_id/dataset_id). Please use the actual project and dataset IDs found in table metadata.',
		});
		await expect(executeSql('SELECT * FROM real_project.dataset_id.table')).resolves.toEqual({
			error: 'Placeholder detected (project_id/dataset_id). Please use the actual project and dataset IDs found in table metadata.',
		});
		expect(postMock).not.toHaveBeenCalled();
	});

	it('blocks non-read statements before execution', async () => {
		await expect(executeSql('DROP TABLE dataset.table')).resolves.toEqual({
			error: 'Only SELECT or WITH statements are allowed for safety.',
		});
		await expect(executeSql('UPDATE dataset.table SET x = 1')).resolves.toEqual({
			error: 'Only SELECT or WITH statements are allowed for safety.',
		});
		expect(postMock).not.toHaveBeenCalled();
	});

	it('passes sidecar error messages through to the agent', async () => {
		postMock.mockResolvedValueOnce({
			data: { status: 'error', message: 'BigQuery said: Unrecognized name: missing_col' },
		});

		await expect(executeSql('SELECT missing_col FROM dataset.table LIMIT 10')).resolves.toEqual({
			error: 'BigQuery said: Unrecognized name: missing_col',
			suggestion: 'Analyze the error and the table metadata to fix the SQL.',
		});
	});

	it('includes FastAPI HTTP error details in connection failures', async () => {
		postMock.mockRejectedValueOnce({
			response: { data: { detail: 'Bad BigQuery credentials' } },
			message: 'Request failed with status code 500',
		});

		await expect(executeSql('SELECT 1 LIMIT 10')).resolves.toEqual({
			error: 'Connection to SQL engine failed: Bad BigQuery credentials',
		});
	});
});
