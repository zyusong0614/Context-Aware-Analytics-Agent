import { z } from 'zod';
import type { LanguageModelV3 } from 'ai';
import type { AnthropicProviderOptions } from '@ai-sdk/anthropic';

export const LLM_PROVIDERS = ['anthropic', 'google', 'openai'] as const;
export type LlmProvider = (typeof LLM_PROVIDERS)[number];

export const llmProviderSchema = z.enum(LLM_PROVIDERS);

export const llmSelectedModelSchema = z.object({
	provider: llmProviderSchema,
	modelId: z.string(),
});

export type LlmSelectedModel = z.infer<typeof llmSelectedModelSchema>;

export type ProviderSettings = { 
	apiKey: string; 
	baseURL?: string; 
	credentials?: Record<string, string> 
};

/** Map each provider to its specific config type */
export type ProviderConfigMap = {
	anthropic: AnthropicProviderOptions;
	google: any; // Add specific types when needed
	openai: any;
};

/** Model definition with provider-specific config type */
export type ProviderModel<P extends LlmProvider> = {
	id: string;
	name: string;
	contextWindow?: number;
	config?: ProviderConfigMap[P];
};

/** Full provider configuration with SDK create function (backend-only) */
export type ProviderConfig<P extends LlmProvider> = {
	envVar: string;
	models: readonly ProviderModel<P>[];
	create: (settings: ProviderSettings, modelId: string) => LanguageModelV3;
	defaultOptions?: ProviderConfigMap[P];
};

export type LlmProvidersType = {
	[P in LlmProvider]: ProviderConfig<P>;
};
