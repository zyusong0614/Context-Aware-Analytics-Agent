import * as fs from 'fs';
import * as path from 'path';
import { tool } from 'ai';
import { z } from 'zod';

export const createContextTools = (projectPath: string) => {
	const databasesDir = path.join(projectPath, 'databases');

	return {
		search_tables: tool({
			description: 'Search for available database tables based on keywords or descriptions.',
			parameters: z.object({
				query: z.string().describe('The keyword or search term to find relevant tables.'),
			}),
			execute: async ({ query }) => {
				if (!fs.existsSync(databasesDir)) return { tables: [] };
				
				const results: string[] = [];
				const scan = (dir: string, relPath: string) => {
					const entries = fs.readdirSync(dir, { withFileTypes: true });
					const hasColumns = fs.existsSync(path.join(dir, 'columns.md'));
					
					if (hasColumns && relPath.toLowerCase().includes(query.toLowerCase())) {
						results.push(relPath);
					}

					for (const entry of entries) {
						if (entry.isDirectory() && entry.name.startsWith('type=bigquery')) {
							scan(path.join(dir, entry.name), path.join(relPath, entry.name));
						} else if (entry.isDirectory() && relPath !== '') {
							scan(path.join(dir, entry.name), path.join(relPath, entry.name));
						}
					}
				};
				
				scan(databasesDir, '');
				return { matches: results.slice(0, 10) };
			},
		}),

		read_table_metadata: tool({
			description: 'Read the detailed schema, AI summary, or usage examples for a specific table.',
			parameters: z.object({
				tableFqdn: z.string().describe('The FQDN of the table (e.g., type=bigquery/database=xxx/schema=xxx/table=xxx)'),
			}),
			execute: async ({ tableFqdn }) => {
				const tableDir = path.resolve(databasesDir, tableFqdn);
				if (!tableDir.startsWith(databasesDir) || !fs.existsSync(tableDir)) {
					return { error: 'Table metadata not found.' };
				}

				const files = ['columns.md', 'ai_summary.md', 'how_to_use.md'];
				let content = '';
				for (const file of files) {
					const fp = path.join(tableDir, file);
					if (fs.existsSync(fp)) {
						content += `\n--- ${file} ---\n${fs.readFileSync(fp, 'utf8')}\n`;
					}
				}
				return { content };
			},
		}),
	};
};
