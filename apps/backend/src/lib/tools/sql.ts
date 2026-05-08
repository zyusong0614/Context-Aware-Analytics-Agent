import { tool } from 'ai';
import { z } from 'zod';
import axios from 'axios';

export const createSqlTools = (sidecarUrl: string) => {
	return {
		execute_bigquery_sql: tool({
			description: 'Execute a SELECT query against BigQuery. Ensure the SQL is valid and contains a LIMIT clause.',
			parameters: z.object({
				sql: z.string().describe('The SELECT SQL statement to execute.'),
			}),
			execute: async ({ sql }) => {
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
					const response = await axios.post(`${sidecarUrl}/api/ibis/execute`, {
						sql: finalSql,
						backend: 'bigquery'
					});

					const { data } = response;
					if (data.status === 'error') {
						return { 
							error: data.message, 
							suggestion: 'Analyze the error and the table metadata to fix the SQL.' 
						};
					}

					return {
						results: data.results,
						columns: data.columns,
						rowCount: data.results.length,
						message: data.results.length >= 100 ? 'Query results truncated to 100 rows.' : 'Success'
					};
				} catch (e: any) {
					return { error: `Connection to SQL engine failed: ${e.message}` };
				}
			},
		}),
	};
};
