import * as fs from 'fs';
import * as path from 'path';
import { tool } from 'ai';
import { z } from 'zod';

export const createContextTools = (projectPath: string) => {
	const databasesDir = path.join(projectPath, 'databases');

	return {
		search_tables: tool({
			description: 'Search for available database tables based on keywords or descriptions.',
			inputSchema: z.object({
				query: z.string().describe('The keyword or search term to find relevant tables.'),
			}),
			execute: async ({ query }: { query: string }) => {
				if (!fs.existsSync(databasesDir)) return { matches: [] };
				
				const results: string[] = [];
				// Normalize query: spaces → underscores, trim
				const normalized = query.trim().toLowerCase().replace(/\s+/g, '_');
				const isWildcard = normalized === '*' || normalized === '';

				const scan = (dir: string, relPath: string) => {
					const entries = fs.readdirSync(dir, { withFileTypes: true });
					const hasColumns = fs.existsSync(path.join(dir, 'columns.md'));
					
					if (hasColumns && relPath !== '' && (isWildcard || relPath.toLowerCase().includes(normalized))) {
						results.push(relPath);
					}

					for (const entry of entries) {
						if (entry.isDirectory() && !entry.name.startsWith('.')) {
							const childRel = relPath ? path.join(relPath, entry.name) : entry.name;
							scan(path.join(dir, entry.name), childRel);
						}
					}
				};
				
				scan(databasesDir, '');

				// If no exact match found, return all tables as fallback
				if (results.length === 0 && !isWildcard) {
					scan(databasesDir, '');
					// Re-scan with wildcard
					const allResults: string[] = [];
					const scanAll = (dir: string, relPath: string) => {
						const entries = fs.readdirSync(dir, { withFileTypes: true });
						if (fs.existsSync(path.join(dir, 'columns.md')) && relPath !== '') {
							allResults.push(relPath);
						}
						for (const entry of entries) {
							if (entry.isDirectory() && !entry.name.startsWith('.')) {
								scanAll(path.join(dir, entry.name), relPath ? path.join(relPath, entry.name) : entry.name);
							}
						}
					};
					scanAll(databasesDir, '');
					return { matches: allResults.slice(0, 20), note: `No exact match for '${query}', showing all available tables.` };
				}

				return { matches: results.slice(0, 20) };
			},
		}),

		read_table_metadata: tool({
			description: 'Read the detailed schema, AI summary, or usage examples for a specific table.',
			inputSchema: z.object({
				tableFqdn: z.string().describe('The FQDN of the table (e.g., type=bigquery/database=xxx/schema=xxx/table=xxx)'),
			}),
			execute: async ({ tableFqdn }: { tableFqdn: string }) => {
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
