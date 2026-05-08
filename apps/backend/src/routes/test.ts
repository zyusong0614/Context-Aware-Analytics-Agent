import { FastifyInstance, FastifyPluginAsync } from 'fastify';

import type { LlmProvider } from '../types/llm';
import { resolveProjectLlmConfig } from '../lib/project-config';

export const testRoutes: FastifyPluginAsync = async (fastify: FastifyInstance) => {
	fastify.post('/run', async (request: any, reply) => {
		const { model, prompt } = request.body || {};
		if (!prompt || typeof prompt !== 'string') {
			return reply.status(400).send({ message: 'prompt is required' });
		}

		const projectPath = process.env.CA3_DEFAULT_PROJECT_PATH || `${process.cwd()}/../../cli/redlake-ca3`;
		const llm = resolveProjectLlmConfig(projectPath, {
			provider: model?.provider as LlmProvider | undefined,
			modelId: model?.modelId,
		});

		const textParts: string[] = [];
		const toolCalls: Array<Record<string, unknown>> = [];
		const startedAt = Date.now();

		try {
			const { AgentManager } = await import('../lib/agents/agent-manager');
			const manager = new AgentManager(llm.provider, llm.modelId, projectPath, { apiKey: llm.apiKey });

			await manager.streamResponse(prompt, (event) => {
				if (event.type === 'message_delta' && typeof event.content === 'string') {
					textParts.push(event.content);
				}
				if (event.type === 'tool_result') {
					toolCalls.push({
						toolName: event.toolName,
						input: event.input,
						output: event.output,
					});
				}
			});

			return {
				text: textParts.join(''),
				toolCalls,
				usage: { totalTokens: 0 },
				cost: { totalCost: 0 },
				finishReason: 'stop',
				durationMs: Date.now() - startedAt,
				verification: null,
			};
		} catch (e: any) {
			return reply.status(500).send({ message: e.message });
		}
	});
};
