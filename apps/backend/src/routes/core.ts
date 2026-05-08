import { FastifyInstance, FastifyPluginAsync } from 'fastify';
import { db } from '../db';
import { chats, messages } from '../db/schema';
import { eq, desc } from 'drizzle-orm';
import { v4 as uuidv4 } from 'uuid';
import * as fs from 'fs';
import * as path from 'path';
import { loadEvalCases, listLatestEvalResults, modelLabel, saveEvalResult, verifyEvalCase, type EvalToolCall } from '../lib/evals';
import { getProjectDir, readProjectConfig, resolveProjectLlmConfig } from '../lib/project-config';
import type { LlmProvider } from '../types/llm';

export const coreRoutes: FastifyPluginAsync = async (fastify: FastifyInstance) => {
	// 1. GET /api/core/project
	fastify.get('/project', async (request, reply) => {
		try {
			const dir = getProjectDir();
			const config = readProjectConfig(dir);
			
			const projectInfo = {
				id: config.project_name || 'ca3-project',
				name: config.project_name || 'CA3 Project',
				path: dir,
				llm: config.llm || null,
				databases: config.databases || []
			};
			
			return { status: 'ok', project: projectInfo };
		} catch (e: any) {
			return reply.status(500).send({ status: 'error', message: e.message });
		}
	});

	// 2. GET /api/core/tables
	fastify.get('/tables', async (request, reply) => {
		try {
			const dir = getProjectDir();
			const databasesDir = path.join(dir, 'databases');
			if (!fs.existsSync(databasesDir)) {
				return { status: 'ok', tables: [] };
			}
			
			const tables: any[] = [];
			function scan(currentDir: string, relPath: string) {
				const entries = fs.readdirSync(currentDir, { withFileTypes: true });
				
				// Check if columns.md exists
				const hasColumns = fs.existsSync(path.join(currentDir, 'columns.md'));
				
				if (hasColumns) {
					const parts = relPath.split(path.sep).filter(Boolean);
					const getVal = (prefix: string, fallback: string) => {
						const part = parts.find(p => p.startsWith(prefix + '='));
						return part ? part.split('=')[1] : fallback;
					};

					tables.push({
						database: getVal('database', parts.length > 2 ? parts[parts.length - 3] : 'default'),
						schema: getVal('schema', parts.length > 1 ? parts[parts.length - 2] : 'default'),
						table: getVal('table', parts[parts.length - 1]),
						fqdn: relPath.replace(new RegExp(`\\${path.sep}`, 'g'), '__DOT__')
					});
					// Continue scanning subdirectories even if this is a table (though unlikely)
				}

				for (const entry of entries) {
					if (entry.isDirectory()) {
						// Only scan type=bigquery directories at the top level to hide other sources
						if (relPath === '' && !entry.name.startsWith('type=bigquery')) {
							continue;
						}
						scan(path.join(currentDir, entry.name), path.join(relPath, entry.name));
					}
				}
			}
			scan(databasesDir, '');
			return { status: 'ok', tables };
		} catch (e: any) {
			return reply.status(500).send({ status: 'error', message: e.message });
		}
	});

	// 3. GET /api/core/tables/:fqdn
	fastify.get('/tables/:fqdn', async (request: any, reply) => {
		try {
			const dir = getProjectDir();
			const databasesDir = path.join(dir, 'databases');
			const encodedFqdn = request.params.fqdn;
			
			// Support both real dots and __DOT__ as separators
			const relPath = encodedFqdn.replace(/__DOT__/g, path.sep).replace(/\./g, path.sep);
			const tableDir = path.resolve(databasesDir, relPath);
			
			// Path Traversal Protection
			if (!tableDir.startsWith(databasesDir)) {
				console.warn(`[Security] Blocked path traversal attempt: ${tableDir}`);
				return reply.status(403).send({ status: 'error', message: 'Forbidden: Path traversal detected' });
			}
			
			console.log(`[Core] Fetching table metadata: ${relPath}`);
			
			if (!fs.existsSync(tableDir)) {
				return reply.status(404).send({ status: 'error', message: `Table not found at ${relPath}` });
			}

			const readSafe = (filename: string) => {
				const fp = path.join(tableDir, filename);
				// Extra check for the individual file read
				if (!fp.startsWith(tableDir)) return null;
				return fs.existsSync(fp) ? fs.readFileSync(fp, 'utf8') : null;
			};

			return { 
				status: 'ok', 
				table: {
					fqdn: encodedFqdn,
					columns: readSafe('columns.md'),
					preview: readSafe('preview.md'),
					profiling: readSafe('profiling.md'),
					howToUse: readSafe('how_to_use.md')
				}
			};
		} catch (e: any) {
			return reply.status(500).send({ status: 'error', message: e.message });
		}
	});

	// 4. GET /api/core/chats
	fastify.get('/chats', async () => {
		const allChats = db.select().from(chats).orderBy(desc(chats.updatedAt)).all();
		return { status: 'ok', chats: allChats };
	});

	// 5. GET /api/core/chats/:chatId
	fastify.get('/chats/:chatId', async (request: any, reply) => {
		const { chatId } = request.params;
		const chat = db.select().from(chats).where(eq(chats.id, chatId)).get();
		if (!chat) {
			return reply.status(404).send({ status: 'error', message: 'Chat not found' });
		}
		const chatMessages = db.select().from(messages).where(eq(messages.chatId, chatId)).orderBy(messages.createdAt).all();
		return { status: 'ok', chat, messages: chatMessages };
	});

	// 6. POST /api/core/agent
	fastify.post('/agent', async (request: any, reply) => {
		const body = request.body || {};
		const { message } = body;
		
		let chatId = body.chatId;
		if (!chatId) {
			chatId = uuidv4();
			db.insert(chats).values({
				id: chatId,
				title: message.substring(0, 50),
				projectId: 'ca3-project'
			}).run();
		}

		// Save user message
		db.insert(messages).values({
			id: uuidv4(),
			chatId,
			role: 'user',
			content: message
		}).run();

		const projectPath = getProjectDir();
		const llm = resolveProjectLlmConfig(projectPath);

		reply.raw.setHeader('Content-Type', 'text/event-stream');
		reply.raw.setHeader('Cache-Control', 'no-cache');
		reply.raw.setHeader('Connection', 'keep-alive');

		const sendEvent = (data: any) => {
			reply.raw.write(`data: ${JSON.stringify(data)}\n\n`);
		};

		let fullResponse = '';

		try {
			const { AgentManager } = await import('../lib/agents/agent-manager');
			const manager = new AgentManager(llm.provider, llm.modelId, projectPath, { apiKey: llm.apiKey });
			
			await manager.streamResponse(message, (data) => {
				if (data.type === 'message_delta') {
					fullResponse += data.content;
				}
				sendEvent({ ...data, chatId });
			});

			// Save assistant response
			if (fullResponse) {
				db.insert(messages).values({
					id: uuidv4(),
					chatId,
					role: 'assistant',
					content: fullResponse
				}).run();
			}
		} catch (e: any) {
			sendEvent({ type: 'error', message: e.message });
		} finally {
			reply.raw.end();
		}
	});

	// 7. POST /api/core/agent/stop
	fastify.post('/agent/stop', async () => {
		return { status: 'ok' };
	});

	// 8. GET /api/core/evals
	fastify.get('/evals', async (_request, reply) => {
		try {
			const projectDir = getProjectDir();
			const latestResults = listLatestEvalResults(projectDir);
			const evals = loadEvalCases(projectDir).map(testCase => ({
				...testCase,
				lastResult: latestResults.get(testCase.id) ?? null,
			}));

			return { status: 'ok', evals };
		} catch (e: any) {
			return reply.status(500).send({ status: 'error', message: e.message });
		}
	});

	// 9. POST /api/core/evals/run
	fastify.post('/evals/run', async (request: any, reply) => {
		const { id, model } = request.body || {};
		if (!id || typeof id !== 'string') {
			return reply.status(400).send({ status: 'error', message: 'id is required' });
		}
		const projectDir = getProjectDir();
		
		try {
			const testCase = loadEvalCases(projectDir).find(item => item.id === id);
			if (!testCase) {
				return reply.status(404).send({ status: 'error', message: `Evaluation not found: ${id}` });
			}

			const llm = resolveProjectLlmConfig(projectDir, {
				provider: model?.provider as LlmProvider | undefined,
				modelId: model?.modelId,
			});
			const textParts: string[] = [];
			const toolCalls: EvalToolCall[] = [];
			const startedAt = Date.now();

			const { AgentManager } = await import('../lib/agents/agent-manager');
			const manager = new AgentManager(llm.provider, llm.modelId, projectDir, { apiKey: llm.apiKey });
			await manager.streamResponse(testCase.question, (event) => {
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

			const result = verifyEvalCase({
				testCase,
				toolCalls,
				responseText: textParts.join(''),
				model: modelLabel(llm.provider, llm.modelId),
				durationMs: Date.now() - startedAt,
			});
			saveEvalResult(projectDir, result);

			return { status: 'ok', result };
		} catch (e: any) {
			console.error('Eval Error:', e);
			return reply.status(500).send({ status: 'error', message: e.message });
		}
	});
};
