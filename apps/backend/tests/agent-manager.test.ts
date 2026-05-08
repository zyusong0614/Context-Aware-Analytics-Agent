import { beforeEach, describe, expect, it, vi } from 'vitest';

function streamFrom(parts: any[]) {
	return {
		async *[Symbol.asyncIterator]() {
			for (const part of parts) {
				yield part;
			}
		},
	};
}

describe('AgentManager stream handling', () => {
	const streamTextMock = vi.fn();
	const stepCountIsMock = vi.fn((count: number) => ({ type: 'step-count', count }));
	const createProviderModelMock = vi.fn(() => ({ model: { provider: 'mock-model' }, contextWindow: 200000 }));

	beforeEach(() => {
		vi.resetModules();
		vi.clearAllMocks();
		process.env.ANTHROPIC_API_KEY = 'test-key';

		vi.doMock('ai', async (importOriginal) => ({
			...(await importOriginal<typeof import('ai')>()),
			streamText: streamTextMock,
			stepCountIs: stepCountIsMock,
		}));

		vi.doMock('../src/lib/agents/providers', () => ({
			LLM_PROVIDERS: {
				anthropic: { envVar: 'ANTHROPIC_API_KEY', models: [], create: vi.fn() },
				google: { envVar: 'GOOGLE_GENERATIVE_AI_API_KEY', models: [], create: vi.fn() },
				openai: { envVar: 'OPENAI_API_KEY', models: [], create: vi.fn() },
			},
			createProviderModel: createProviderModelMock,
		}));
	});

	it('uses the requested provider and AI SDK 6 step stop condition', async () => {
		streamTextMock.mockResolvedValueOnce({
			fullStream: streamFrom([{ type: 'finish', finishReason: 'stop', totalUsage: { inputTokens: 1, outputTokens: 1 } }]),
		});

		const { AgentManager } = await import('../src/lib/agents/agent-manager');
		const manager = new AgentManager('anthropic', 'claude-haiku-4-5-20251001', '/tmp/project');
		await manager.streamResponse('hello', vi.fn());

		expect(createProviderModelMock).toHaveBeenCalledWith(
			'anthropic',
			{ apiKey: 'test-key' },
			'claude-haiku-4-5-20251001',
		);
		expect(stepCountIsMock).toHaveBeenCalledWith(10);
		expect(streamTextMock.mock.calls[0][0]).toMatchObject({
			model: { provider: 'mock-model' },
			stopWhen: { type: 'step-count', count: 10 },
		});
		expect(Object.keys(streamTextMock.mock.calls[0][0].tools)).toEqual([
			'search_tables',
			'read_table_metadata',
			'execute_bigquery_sql',
		]);
	});

	it('emits text from the AI SDK 6 text field and never stringifies undefined deltas', async () => {
		streamTextMock.mockResolvedValueOnce({
			fullStream: streamFrom([
				{ type: 'tool-call', toolName: 'search_tables', input: { query: 'tech keywords' } },
				{ type: 'text-delta', text: 'There are ' },
				{ type: 'text-delta', text: '42 keywords.' },
				{ type: 'finish', finishReason: 'stop', totalUsage: {} },
			]),
		});

		const events: any[] = [];
		const { AgentManager } = await import('../src/lib/agents/agent-manager');
		const manager = new AgentManager('anthropic', 'claude-haiku-4-5-20251001', '/tmp/project');
		await manager.streamResponse('How many tech keywords?', (event) => events.push(event));

		expect(events).toEqual([
			{ type: 'status', message: 'Calling tool: search_tables...' },
			{ type: 'message_delta', content: 'There are ' },
			{ type: 'message_delta', content: '42 keywords.' },
			{ type: 'final' },
		]);
		expect(events.map((event) => event.content).join('')).not.toContain('undefined');
	});

	it('forwards tool results with toolName, input, and output for the inspector', async () => {
		streamTextMock.mockResolvedValueOnce({
			fullStream: streamFrom([
				{
					type: 'tool-result',
					toolName: 'execute_bigquery_sql',
					input: { sql: 'SELECT 1 LIMIT 100' },
					output: { results: [{ count: 1 }], rowCount: 1 },
				},
				{ type: 'finish', finishReason: 'stop', totalUsage: {} },
			]),
		});

		const events: any[] = [];
		const { AgentManager } = await import('../src/lib/agents/agent-manager');
		const manager = new AgentManager('anthropic', 'claude-haiku-4-5-20251001', '/tmp/project');
		await manager.streamResponse('run sql', (event) => events.push(event));

		expect(events[0]).toEqual({
			type: 'tool_result',
			toolName: 'execute_bigquery_sql',
			input: { sql: 'SELECT 1 LIMIT 100' },
			output: { results: [{ count: 1 }], rowCount: 1 },
		});
	});
});
