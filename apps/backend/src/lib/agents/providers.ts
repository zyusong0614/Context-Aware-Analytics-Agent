import { createAnthropic } from '@ai-sdk/anthropic';
import { createGoogleGenerativeAI } from '@ai-sdk/google';
import { createOpenAI } from '@ai-sdk/openai';
import type { LlmProvidersType, ProviderSettings } from '../../types/llm';

export const LLM_PROVIDERS: LlmProvidersType = {
	anthropic: {
		envVar: 'ANTHROPIC_API_KEY',
		models: [
			{ id: 'claude-opus-4-7', name: 'Claude 4.7 Opus', contextWindow: 200000 },
			{ id: 'claude-sonnet-4-6', name: 'Claude 4.6 Sonnet', contextWindow: 200000 },
			{ id: 'claude-haiku-4-5-20251001', name: 'Claude 4.5 Haiku', contextWindow: 200000 },
		],
		create: (settings: ProviderSettings, modelId: string) => createAnthropic(settings)(modelId),
		defaultOptions: {
			// Enable prompt caching by default for Anthropic
		}
	},
	google: {
		envVar: 'GOOGLE_GENERATIVE_AI_API_KEY',
		models: [
			{ id: 'gemini-2.5-pro', name: 'Gemini 2.5 Pro', contextWindow: 1000000 },
			{ id: 'gemini-2.5-flash', name: 'Gemini 2.5 Flash', contextWindow: 1000000 },
		],
		create: (settings: ProviderSettings, modelId: string) => createGoogleGenerativeAI(settings)(modelId)
	},
	openai: {
		envVar: 'OPENAI_API_KEY',
		models: [
			{ id: 'gpt-5.2', name: 'GPT-5.2', contextWindow: 400000 },
			{ id: 'gpt-5.4-mini', name: 'GPT-5.4 Mini', contextWindow: 400000 },
		],
		create: (settings: ProviderSettings, modelId: string) => createOpenAI(settings)(modelId)
	}
};

export function createProviderModel(
	provider: 'anthropic' | 'google' | 'openai',
	settings: ProviderSettings,
	modelId: string
) {
	const config = LLM_PROVIDERS[provider];
	return {
		model: config.create(settings, modelId),
		contextWindow: config.models.find(m => m.id === modelId)?.contextWindow ?? 200000
	};
}
