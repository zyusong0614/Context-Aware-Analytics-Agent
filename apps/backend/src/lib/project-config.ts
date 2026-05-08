import fs from 'node:fs';
import path from 'node:path';

import * as yaml from 'js-yaml';

import { env } from '../env';
import { LLM_PROVIDERS, type LlmProvider } from '../types/llm';

export type ProjectConfig = {
	project_name?: string;
	llm?: {
		provider?: string;
		model?: string;
		model_id?: string;
		modelId?: string;
		annotation_model?: string;
		api_key?: string;
	};
	databases?: unknown[];
};

export type ResolvedLlmConfig = {
	provider: LlmProvider;
	modelId: string;
	apiKey?: string;
};

const DEFAULT_LLM_CONFIG: ResolvedLlmConfig = {
	provider: 'anthropic',
	modelId: 'claude-haiku-4-5-20251001',
};

export function getProjectDir() {
	const dir = env.CA3_DEFAULT_PROJECT_PATH || path.join(process.cwd(), '../../cli/redlake-ca3');
	if (!fs.existsSync(dir)) {
		throw new Error(`Project directory not found: ${dir}`);
	}
	return dir;
}

export function readProjectConfig(projectPath = getProjectDir()): ProjectConfig {
	const configPath = path.join(projectPath, 'ca3_config.yaml');
	if (!fs.existsSync(configPath)) {
		return {};
	}
	return (yaml.load(fs.readFileSync(configPath, 'utf8')) as ProjectConfig | null) || {};
}

function isLlmProvider(provider: string | undefined): provider is LlmProvider {
	return !!provider && (LLM_PROVIDERS as readonly string[]).includes(provider);
}

export function resolveProjectLlmConfig(
	projectPath = getProjectDir(),
	overrides: Partial<ResolvedLlmConfig> = {},
): ResolvedLlmConfig {
	const config = readProjectConfig(projectPath);
	const configuredProvider = config.llm?.provider;
	const provider = overrides.provider ?? (isLlmProvider(configuredProvider) ? configuredProvider : DEFAULT_LLM_CONFIG.provider);
	const modelId = overrides.modelId
		?? config.llm?.modelId
		?? config.llm?.model_id
		?? config.llm?.model
		?? config.llm?.annotation_model
		?? DEFAULT_LLM_CONFIG.modelId;

	return {
		provider,
		modelId,
		apiKey: overrides.apiKey ?? config.llm?.api_key,
	};
}
