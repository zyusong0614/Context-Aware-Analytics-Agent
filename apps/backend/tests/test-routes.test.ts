import fastify from 'fastify';
import { beforeEach, describe, expect, it, vi } from 'vitest';

describe('test routes', () => {
	beforeEach(() => {
		vi.resetModules();
		vi.clearAllMocks();
	});

	it('runs a prompt through AgentManager and returns the CLI result shape', async () => {
		vi.doMock('../src/lib/agents/agent-manager', () => ({
			AgentManager: class {
				async streamResponse(_prompt: string, onProgress: (event: any) => void) {
					onProgress({ type: 'tool_result', toolName: 'search_tables', input: { query: 'tech' }, output: { matches: ['table'] } });
					onProgress({ type: 'message_delta', content: '42' });
				}
			},
		}));

		const { testRoutes } = await import('../src/routes/test');
		const app = fastify();
		await app.register(testRoutes, { prefix: '/api/test' });

		const response = await app.inject({
			method: 'POST',
			url: '/api/test/run',
			payload: {
				model: { provider: 'anthropic', modelId: 'claude-haiku-4-5-20251001' },
				prompt: 'How many tech keywords?',
			},
		});

		expect(response.statusCode).toBe(200);
		expect(response.json()).toMatchObject({
			text: '42',
			toolCalls: [
				{
					toolName: 'search_tables',
					input: { query: 'tech' },
					output: { matches: ['table'] },
				},
			],
			usage: { totalTokens: 0 },
			cost: { totalCost: 0 },
			finishReason: 'stop',
			verification: null,
		});

		await app.close();
	});
});
