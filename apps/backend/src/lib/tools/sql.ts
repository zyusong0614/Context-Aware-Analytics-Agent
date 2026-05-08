import { tool } from 'ai';
import { z } from 'zod';
import axios from 'axios';

export const createSqlTools = (sidecarUrl: string, projectPath?: string) => {
	return {
		execute_bigquery_sql: tool({
			description: 'Execute a SELECT query against BigQuery. Ensure the SQL is valid and contains a LIMIT clause.',
			inputSchema: z.object({
				sql: z.string().describe('The SELECT SQL statement to execute.'),
			}),
			execute: async ({ sql }: { sql: string }) => {
				// 1. Pre-execution Validation
				const upperSql = sql.toUpperCase().trim();
				
				if (!upperSql.startsWith('SELECT') && !upperSql.startsWith('WITH')) {
					return { error: 'Only SELECT or WITH statements are allowed for safety.' };
				}

				if (sql.includes('project_id') || sql.includes('dataset_id')) {
					return { error: 'Placeholder detected (project_id/dataset_id). Please use the actual project and dataset IDs found in table metadata.' };
				}

				// 2. Mandatory LIMIT injection if missing
				let finalSql = sql;
				if (!upperSql.includes('LIMIT')) {
					finalSql = `${sql.trim().replace(/;$/, '')} LIMIT 100`;
				}

				// 3. Execution via Sidecar
				try {
					const response = await axios.post(`${sidecarUrl}/execute_sql`, {
						sql: finalSql,
						ca3_project_folder: projectPath ?? process.env.CA3_DEFAULT_PROJECT_PATH ?? `${process.cwd()}/../../cli/redlake-ca3`,
					});

					const { data } = response;
					if (data.status === 'error' || data.error) {
						return { 
							error: data.message ?? data.error, 
							suggestion: 'Analyze the error and the table metadata to fix the SQL.' 
						};
					}

					return {
						sql: finalSql,
						results: data.data ?? data.results ?? [],
						columns: data.columns,
						rowCount: data.row_count ?? data.results?.length ?? data.data?.length ?? 0,
						message: (data.row_count ?? data.data?.length ?? 0) >= 100 ? 'Query results truncated to 100 rows.' : 'Success'
					};
				} catch (e: any) {
					const detail = e.response?.data?.detail ?? e.response?.data?.message ?? e.message;
					return { error: `Connection to SQL engine failed: ${detail}` };
				}
			},
		}),
	};
};
