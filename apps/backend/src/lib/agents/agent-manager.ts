import { streamText, type LanguageModelV3 } from 'ai';
import { createContextTools } from '../tools/context';
import { createSqlTools } from '../tools/sql';
import { createProviderModel } from './providers';
import { env } from '../../env';

export class AgentManager {
	private model: LanguageModelV3;
	private projectPath: string;

	constructor(provider: any, modelId: string, projectPath: string) {
		const apiKey = env.ANTHROPIC_API_KEY; // Fallback to env for now
		const { model } = createProviderModel(provider, { apiKey: apiKey! }, modelId);
		this.model = model;
		this.projectPath = projectPath;
	}

	async streamResponse(message: string, onProgress: (data: any) => void) {
		const contextTools = createContextTools(this.projectPath);
		const sqlTools = createSqlTools('http://localhost:8005');

		const result = await streamText({
			model: this.model,
			maxSteps: 10,
			system: `You are a professional BigQuery Data Analyst.
Your goal is to answer the user's question by generating and executing SQL.

PHASE 1: DISCOVERY
If you don't know the table schema, use 'search_tables' to find relevant tables.
Then use 'read_table_metadata' to understand the columns and business context.

PHASE 2: EXECUTION
Write a standard SQL query and use 'execute_bigquery_sql'.
If the execution fails, analyze the error, re-read metadata if necessary, and try again.

CRITICAL: 
- Always use the fully qualified table name (project.dataset.table).
- Never hallucinate table names.
- If you find placeholder like 'project_id' in metadata summaries, replace them with the actual IDs from the file path context.`,
			messages: [{ role: 'user', content: message }],
			tools: {
				...contextTools,
				...sqlTools
			},
			onStepFinish: (step) => {
				onProgress({ type: 'status', message: `Step finished: ${step.toolCalls.length} tools called.` });
				if (step.toolResults.length > 0) {
					onProgress({ type: 'tool_result', data: step.toolResults });
				}
			}
		});

		for await (const delta of result.fullStream) {
			if (delta.type === 'text-delta') {
				onProgress({ type: 'message_delta', content: delta.textDelta });
			}
		}
	}
}
