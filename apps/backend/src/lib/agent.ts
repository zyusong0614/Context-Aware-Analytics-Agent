import Anthropic from '@anthropic-ai/sdk';
import * as fs from 'fs';
import * as path from 'path';

export async function runAgent(
	message: string,
	projectPath: string,
	apiKey: string,
	onProgress: (data: any) => void
) {
	const anthropic = new Anthropic({ apiKey });

	try {
		// 1. Retrieval
		onProgress({ type: 'status', message: 'Analyzing database context...' });
		const databasesDir = path.join(projectPath, 'databases');
		let context = '';
		if (fs.existsSync(databasesDir)) {
			const scan = (dir: string) => {
				const entries = fs.readdirSync(dir, { withFileTypes: true });
				for (const entry of entries) {
					const fullPath = path.join(dir, entry.name);
					if (entry.isDirectory()) scan(fullPath);
					else if (entry.name.endsWith('.md')) {
						const fileName = entry.name;
						context += `\n[Context from ${fileName}]:\n${fs.readFileSync(fullPath, 'utf8')}\n`;
					}
				}
			};
			scan(databasesDir);
		}

		let currentAttempt = 0;
		const maxAttempts = 10;
		let previousErrors = '';
		let finalResult = null;

		while (currentAttempt < maxAttempts) {
			currentAttempt++;
			
			// 2. SQL Generation
			onProgress({ type: 'status', message: currentAttempt === 1 ? 'Generating SQL query...' : `Self-correcting (Attempt ${currentAttempt})...` });
			
			const systemPrompt = `You are a BigQuery expert. 
CRITICAL: Output ONLY the SQL code block starting with \`\`\`sql. 
Do NOT provide any explanations, headers, or markdown outside the SQL block.
Use fully qualified table names (project.dataset.table).
If you saw errors in previous attempts, fix them.

Schema Context:
${context}

${previousErrors ? `Previous Errors:\n${previousErrors}` : ''}`;

			let msg;
			let apiRetry = 0;
			while (apiRetry < 3) {
				try {
					msg = await anthropic.messages.create({
						model: 'claude-haiku-4-5-20251001',
						max_tokens: 1024,
						messages: [{ role: 'user', content: `${systemPrompt}\n\nUser Question: ${message}` }]
					});
					break;
				} catch (e: any) {
					if (e.status === 529 || e.status === 429) {
						apiRetry++;
						onProgress({ type: 'status', message: `Anthropic busy, retrying in ${apiRetry * 2}s...` });
						await new Promise(resolve => setTimeout(resolve, apiRetry * 2000));
					} else throw e;
				}
			}

			if (!msg) throw new Error('Anthropic API is currently unavailable. Please try again later.');

			const sqlMatch = (msg.content[0] as any).text.match(/```sql\n([\s\S]*?)\n```/);
			
			if (!sqlMatch) {
				previousErrors += `\nAttempt ${currentAttempt} Failed: You did not provide a valid SQL block starting with \`\`\`sql.\n`;
				continue;
			}

			const sql = sqlMatch[1].trim();

			onProgress({ type: 'sql', sql });

			// --- Pre-Execution Validation ---
			let validationError = null;
			if (!sql.toUpperCase().startsWith('SELECT') && !sql.toUpperCase().startsWith('WITH')) {
				validationError = 'SQL must be a SELECT or WITH statement for security.';
			} else if (sql.includes('project_id') || sql.includes('dataset_id') || sql.includes('table_name')) {
				validationError = 'SQL contains placeholders (project_id/dataset_id). Please replace them with real IDs from the schema context.';
			}

			if (validationError) {
				previousErrors += `\nAttempt ${currentAttempt} Validation Failed: ${validationError}\n`;
				continue; // Skip execution, trigger self-correction
			}

			// 3. Execution
			onProgress({ type: 'status', message: 'Executing on BigQuery...' });
			const res = await fetch('http://localhost:8005/execute_sql', {
				method: 'POST',
				headers: { 'Content-Type': 'application/json' },
				body: JSON.stringify({ sql, ca3_project_folder: projectPath })
			});

			const result: any = await res.json();
			if (res.ok && !result.error) {
				finalResult = result;
				break; // Success!
			} else {
				const errorMsg = result.error || 'Unknown execution error';
				previousErrors += `\nAttempt ${currentAttempt} SQL: ${sql}\nError: ${errorMsg}\n`;
				if (currentAttempt === maxAttempts) throw new Error(`Failed after ${maxAttempts} attempts. Last error: ${errorMsg}`);
			}
		}

		// 4. Results & Summary
		if (finalResult) {
			onProgress({ type: 'results', data: finalResult.data, columns: finalResult.columns });
			onProgress({ type: 'status', message: 'Summarizing answer...' });
			
			// Truncate data to avoid token limits
			const displayData = finalResult.data.slice(0, 10);
			const totalRows = finalResult.data.length;

			const summaryPrompt = `User Question: ${message}\nSQL Results (showing first 10 of ${totalRows} rows): ${JSON.stringify(displayData)}\nSummarize the findings and mention there are ${totalRows} total rows:`;
			const summaryStream = await anthropic.messages.create({
				model: 'claude-haiku-4-5-20251001',
				max_tokens: 1024,
				messages: [{ role: 'user', content: summaryPrompt }],
				stream: true
			});

			for await (const chunk of summaryStream) {
				if (chunk.type === 'content_block_delta' && (chunk.delta as any).text) {
					onProgress({ type: 'message_delta', content: (chunk.delta as any).text });
				}
			}
		}
	} catch (error: any) {
		console.error('Agent Error:', error);
		onProgress({ type: 'error', message: error.message });
	}

	onProgress({ type: 'final' });
}
