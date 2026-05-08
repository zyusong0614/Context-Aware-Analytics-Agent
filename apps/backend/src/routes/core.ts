import { FastifyInstance, FastifyPluginAsync } from 'fastify';
import { db } from '../db';
import { chats, messages } from '../db/schema';
import { eq, desc } from 'drizzle-orm';
import { v4 as uuidv4 } from 'uuid';
import { env } from '../env';
import { runAgent } from '../lib/agent';
import * as fs from 'fs';
import * as path from 'path';
import * as yaml from 'js-yaml';

export const coreRoutes: FastifyPluginAsync = async (fastify: FastifyInstance) => {
	
	const getProjectDir = () => {
		const dir = env.CA3_DEFAULT_PROJECT_PATH || path.join(process.cwd(), '../../cli/redlake-ca3');
		if (!fs.existsSync(dir)) {
			throw new Error(`Project directory not found: ${dir}`);
		}
		return dir;
	};

	// 1. GET /api/core/project
	fastify.get('/project', async (request, reply) => {
		try {
			const dir = getProjectDir();
			const configPath = path.join(dir, 'ca3_config.yaml');
			if (!fs.existsSync(configPath)) {
				return { status: 'error', message: 'ca3_config.yaml not found' };
			}
			const configContent = fs.readFileSync(configPath, 'utf8');
			const config = yaml.load(configContent) as any;
			
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
			
			// Replace dots with path separators and normalize
			const relPath = encodedFqdn.replace(/__DOT__/g, path.sep);
			const tableDir = path.resolve(databasesDir, relPath);
			
			// Path Traversal Protection: Ensure the resolved path is within databasesDir
			if (!tableDir.startsWith(databasesDir)) {
				return reply.status(403).send({ status: 'error', message: 'Forbidden: Path traversal detected' });
			}
			
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
	fastify.get('/chats', async (request, reply) => {
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
		const apiKey = env.ANTHROPIC_API_KEY;

		if (!apiKey) {
			return reply.status(500).send({ status: 'error', message: 'ANTHROPIC_API_KEY not configured' });
		}

		reply.raw.setHeader('Content-Type', 'text/event-stream');
		reply.raw.setHeader('Cache-Control', 'no-cache');
		reply.raw.setHeader('Connection', 'keep-alive');

		const sendEvent = (data: any) => {
			reply.raw.write(`data: ${JSON.stringify(data)}\n\n`);
		};

		let fullResponse = '';

		try {
			const { AgentManager } = await import('../lib/agents/agent-manager');
			const manager = new AgentManager('anthropic', 'claude-haiku-4-5-20251001', projectPath);
			
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
	fastify.post('/agent/stop', async (request, reply) => {
		return { status: 'ok' };
	});

	// 8. GET /api/core/evals
	fastify.get('/evals', async (request, reply) => {
		try {
			const projectDir = getProjectDir();
			const testsDir = path.join(projectDir, 'tests');
			if (!fs.existsSync(testsDir)) return { status: 'ok', evals: [] };

			const files = fs.readdirSync(testsDir).filter(f => f.endsWith('.yml') || f.endsWith('.yaml'));
			const evals: any[] = [];

			for (const file of files) {
				const content = fs.readFileSync(path.join(testsDir, file), 'utf8');
				const parsed = yaml.load(content) as any[];
				if (Array.isArray(parsed)) {
					evals.push(...parsed.map(e => ({ ...e, file })));
				}
			}

			return { status: 'ok', evals };
		} catch (e: any) {
			return reply.status(500).send({ status: 'error', message: e.message });
		}
	});

	// 9. POST /api/core/evals/run
	fastify.post('/evals/run', async (request: any, reply) => {
		const { id } = request.body;
		const projectDir = getProjectDir();
		
		try {
			const { exec } = await import('child_process');
			const { promisify } = await import('util');
			const execAsync = promisify(exec);

			// Run the real test command for the specific project
			const cliDir = path.join(projectDir, '..');
			await execAsync('uv run ca3 test', { cwd: cliDir });

			const resultsPath = path.join(projectDir, 'test_results.json');
			if (fs.existsSync(resultsPath)) {
				const allResults = JSON.parse(fs.readFileSync(resultsPath, 'utf8'));
				const testResult = allResults.find((r: any) => r.id === id);
				return { status: 'ok', result: testResult || { status: 'completed', message: 'No specific result found' } };
			}
			
			return { status: 'ok', message: 'Evaluation completed' };
		} catch (e: any) {
			console.error('Eval Error:', e);
			return reply.status(500).send({ status: 'error', message: e.message });
		}
	});
};
