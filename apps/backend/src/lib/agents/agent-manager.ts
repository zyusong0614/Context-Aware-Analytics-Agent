import { streamText, stepCountIs, type LanguageModel } from 'ai';
import type { LlmProvider } from '../../types/llm';
import { createContextTools } from '../tools/context';
import { createSqlTools } from '../tools/sql';
import { createProviderModel, LLM_PROVIDERS } from './providers';
import { env } from '../../env';

export class AgentManager {
	private model: LanguageModel;
	private projectPath: string;

	constructor(provider: LlmProvider, modelId: string, projectPath: string, settings: { apiKey?: string } = {}) {
		const config = LLM_PROVIDERS[provider];
		const apiKey = settings.apiKey ?? env[config.envVar as keyof typeof env];
		if (typeof apiKey !== 'string' || !apiKey) {
			throw new Error(`${config.envVar} not configured`);
		}

		const { model } = createProviderModel(provider, { apiKey }, modelId);
		this.model = model;
		this.projectPath = projectPath;
	}

	async streamResponse(message: string, onProgress: (data: any) => void) {
		const contextTools = createContextTools(this.projectPath);
		const sqlTools = createSqlTools('http://localhost:8005', this.projectPath);

		const result = await streamText({
			model: this.model,
			stopWhen: stepCountIs(10),
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
				...sqlTools,
			},
			onStepFinish: (step) => {
				// Log tool calls for debugging but don't send noisy status to UI
				if (step.toolCalls.length > 0) {
					console.log(`[Agent] Step finished: ${step.toolCalls.map(t => t.toolName).join(', ')}`);
				}
			}
		});

		for await (const delta of result.fullStream) {
			switch (delta.type) {
				case 'text-delta':
					onProgress({ type: 'message_delta', content: delta.text });
					break;
				case 'tool-call':
					console.log(`[Agent] Calling tool: ${delta.toolName}`, delta.input);
					onProgress({ type: 'status', message: `Calling tool: ${delta.toolName}...` });
					break;
				case 'tool-result':
					console.log(`[Agent] Tool result from: ${delta.toolName}`);
					onProgress({
						type: 'tool_result',
						toolName: delta.toolName,
						input: delta.input,
						output: delta.output,
					});
					break;
				case 'error':
					console.error(`[Agent] Stream Error:`, delta.error);
					onProgress({ type: 'error', message: String(delta.error) });
					break;
				case 'finish':
					console.log(`[Agent] Stream Finished. Reason: ${delta.finishReason}. Usage:`, delta.totalUsage);
					onProgress({ type: 'final' });
					break;
			}
		}
	}
}
