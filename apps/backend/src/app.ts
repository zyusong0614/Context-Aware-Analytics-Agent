import fastifyStatic from '@fastify/static';
import fastify from 'fastify';
import { serializerCompiler, validatorCompiler, ZodTypeProvider } from 'fastify-type-provider-zod';
import { existsSync } from 'fs';
import { dirname, join } from 'path';
import { fileURLToPath } from 'url';

import { coreRoutes } from './routes/core';
import { testRoutes } from './routes/test';

// Get the directory of the current module
const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

const app = fastify({
	logger: true,
	bodyLimit: 35 * 1024 * 1024,
}).withTypeProvider<ZodTypeProvider>();

export type App = typeof app;

// Set the validator and serializer compilers
app.setValidatorCompiler(validatorCompiler);
app.setSerializerCompiler(serializerCompiler);

// Register Core API
app.register(coreRoutes, {
	prefix: '/api/core',
});

app.register(testRoutes, {
	prefix: '/api/test',
});

// Serve frontend static files
const possibleStaticPaths = [
	join(__dirname, 'public'),
	join(__dirname, '../../frontend/dist'),
];

const staticRoot = possibleStaticPaths.find((p) => existsSync(p));

if (staticRoot) {
	app.register(fastifyStatic, {
		root: staticRoot,
		prefix: '/',
		wildcard: false,
	});

	app.setNotFoundHandler((request, reply) => {
		if (request.url.startsWith('/api')) {
			reply.status(404).send({ error: 'Not found' });
		} else {
			reply.sendFile('index.html');
		}
	});
}

export const startServer = async (opts: { port: number; host: string }) => {
	const address = await app.listen({ host: opts.host, port: opts.port });
	app.log.info(`Server is running on ${address}`);

	const handleShutdown = async () => {
		process.exit(0);
	};

	process.on('SIGINT', handleShutdown);
	process.on('SIGTERM', handleShutdown);
};

export default app;
