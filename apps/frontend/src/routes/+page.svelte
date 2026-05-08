<script lang="ts">
	import { page } from '$app/state';
	import { onMount } from 'svelte';

	let { children } = $props();
	let chatId = $derived(page.url.searchParams.get('chatId'));
	
	let inputMessage = $state('');
	let messages = $state([
		{ role: 'assistant', content: 'Hello! I am your Context-Aware Analytics Agent. How can I help you query your data today?' }
	]);
	
	let isGenerating = $state(false);
	
	// Inspector state
	let inspectorOpen = $state(true);
	let currentSql = $state('');
	let usedTables = $state([]);
	let queryResults = $state([]);

	onMount(async () => {
		if (chatId) {
			try {
				const res = await fetch(`/api/core/chats/${chatId}`);
				const data = await res.json();
				if (data.status === 'ok') {
					messages = data.messages;
					// Load last message state into inspector if available
					const lastAssistant = [...messages].reverse().find(m => m.role === 'assistant');
					if (lastAssistant) {
						// Logic to parse SQL from content could go here
					}
				}
			} catch (e) {
				console.error('Failed to load chat history', e);
			}
		}
	});

	async function sendMessage() {
		if (!inputMessage.trim() || isGenerating) return;
		
		const msg = inputMessage;
		messages = [...messages, { role: 'user', content: msg }];
		inputMessage = '';
		isGenerating = true;

		// Reset inspector for new query
		currentSql = '';
		usedTables = [];
		queryResults = [];

		// Create assistant placeholder
		messages = [...messages, { role: 'assistant', content: '' }];
		const assistantIdx = messages.length - 1;
		
		try {
			const res = await fetch('/api/core/agent', {
				method: 'POST',
				headers: { 'Content-Type': 'application/json' },
				body: JSON.stringify({ message: msg, chatId })
			});
			
			const reader = res.body?.getReader();
			if (!reader) return;

			const decoder = new TextDecoder();
			let buffer = '';

			while (true) {
				const { done, value } = await reader.read();
				if (done) break;

				buffer += decoder.decode(value, { stream: true });
				const lines = buffer.split('\n');
				buffer = lines.pop() || '';

				for (const line of lines) {
					if (!line.startsWith('data: ')) continue;
					try {
						const data = JSON.parse(line.slice(6));
						
						if (data.type === 'status') {
							messages[assistantIdx].content = `*${data.message}*`;
						} else if (data.type === 'message_delta') {
							if (messages[assistantIdx].content.startsWith('*')) {
								messages[assistantIdx].content = '';
							}
							messages[assistantIdx].content += data.content;
						} else if (data.type === 'sql') {
							currentSql = data.sql;
						} else if (data.type === 'results') {
							queryResults = data.data;
							usedTables = data.tables || [];
						} else if (data.type === 'error') {
							messages[assistantIdx].content = `Error: ${data.message}`;
						}
					} catch (e) {
						console.error('Error parsing SSE:', e);
					}
				}
			}
		} catch (e) {
			console.error(e);
			messages[assistantIdx].content = 'Sorry, an error occurred.';
		} finally {
			isGenerating = false;
		}
	}

	function stopGeneration() {
		isGenerating = false;
		fetch('/api/core/agent/stop', { method: 'POST' });
	}
</script>

<!-- Chat Area -->
<div class="flex-1 flex flex-col min-w-0 bg-neutral-900 relative">
	
	<!-- Header -->
	<header class="h-14 border-b border-neutral-800 flex items-center justify-between px-4 shrink-0">
		<h1 class="font-semibold text-neutral-200">Data Chat</h1>
		<button 
			class="text-neutral-400 hover:text-white p-2 rounded-md hover:bg-neutral-800 transition-colors"
			onclick={() => inspectorOpen = !inspectorOpen}
			title="Toggle Inspector"
		>
			<svg class="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M13 16h-1v-4h-1m1-4h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z"></path></svg>
		</button>
	</header>

	<!-- Messages -->
	<div class="flex-1 overflow-y-auto p-4 space-y-6">
		{#each messages as msg}
			<div class="flex flex-col {msg.role === 'user' ? 'items-end' : 'items-start'}">
				<div class="max-w-[80%] rounded-2xl px-5 py-3 {msg.role === 'user' ? 'bg-emerald-600 text-white' : 'bg-neutral-800 text-neutral-200'}">
					<div class="prose prose-invert prose-sm max-w-none whitespace-pre-wrap">
						{msg.content}
					</div>
				</div>
			</div>
		{/each}
		{#if isGenerating}
			<div class="flex items-start">
				<div class="bg-neutral-800 rounded-2xl px-5 py-4 min-w-[80px]">
					<div class="flex space-x-2 items-center justify-center">
						<div class="w-1.5 h-1.5 bg-emerald-500/60 rounded-full animate-pulse"></div>
						<div class="w-1.5 h-1.5 bg-emerald-500/60 rounded-full animate-pulse" style="animation-delay: 0.2s"></div>
						<div class="w-1.5 h-1.5 bg-emerald-500/60 rounded-full animate-pulse" style="animation-delay: 0.4s"></div>
					</div>
				</div>
			</div>
		{/if}
	</div>

	<!-- Input Area -->
	<div class="p-4 bg-neutral-900 border-t border-neutral-800 shrink-0">
		<div class="max-w-4xl mx-auto relative">
			{#if isGenerating}
				<div class="absolute -top-12 left-1/2 -translate-x-1/2">
					<button 
						onclick={stopGeneration}
						class="flex items-center space-x-2 bg-neutral-800 hover:bg-neutral-700 text-neutral-300 px-4 py-1.5 rounded-full text-sm border border-neutral-700 shadow-lg transition-colors"
					>
						<div class="w-2 h-2 bg-red-500 rounded-sm"></div>
						<span>Stop generating</span>
					</button>
				</div>
			{/if}
			<form onsubmit={(e) => { e.preventDefault(); sendMessage(); }} class="relative flex items-center bg-neutral-800 border border-neutral-700 rounded-2xl overflow-hidden focus-within:ring-2 focus-within:ring-emerald-500/50 focus-within:border-emerald-500 transition-all shadow-xl">
				<textarea 
					bind:value={inputMessage}
					placeholder="Ask a question about your data..."
					class="w-full max-h-48 min-h-[64px] py-5 pl-6 pr-14 bg-transparent text-neutral-100 placeholder-neutral-500 focus:outline-none resize-none text-base leading-relaxed"
					rows="1"
					onkeydown={(e) => { if(e.key === 'Enter' && !e.shiftKey) { e.preventDefault(); sendMessage(); } }}
				></textarea>
				<button 
					type="submit" 
					disabled={!inputMessage.trim() || isGenerating}
					class="absolute right-3 p-2.5 text-white bg-emerald-600 rounded-xl disabled:opacity-20 disabled:grayscale hover:bg-emerald-500 active:scale-95 transition-all shadow-lg"
				>
					<svg class="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M14 5l7 7m0 0l-7 7m7-7H3"></path></svg>
				</button>
			</form>
			<div class="text-center mt-2">
				<span class="text-xs text-neutral-500">CA3 can make mistakes. Verify important information.</span>
			</div>
		</div>
	</div>
</div>

<!-- Right Inspector -->
{#if inspectorOpen}
<aside class="w-80 bg-neutral-900 border-l border-neutral-800 flex flex-col shrink-0 shadow-xl overflow-hidden">
	<header class="h-14 border-b border-neutral-800 flex items-center px-4 bg-neutral-950">
		<h2 class="font-medium text-sm text-neutral-200">Inspector</h2>
	</header>
	
	<div class="flex-1 overflow-y-auto p-4 space-y-6">
		
		<!-- Context Tables -->
		<section>
			<h3 class="text-xs font-semibold text-neutral-500 uppercase tracking-wider mb-3">Retrieved Tables</h3>
			{#if usedTables.length > 0}
				<div class="space-y-2">
					{#each usedTables as table}
						<div class="bg-neutral-800 border border-neutral-700 rounded-md p-2 text-xs text-neutral-300 font-mono break-all">
							{table}
						</div>
					{/each}
				</div>
			{:else}
				<div class="text-sm text-neutral-500 italic">No tables retrieved yet.</div>
			{/if}
		</section>

		<!-- Generated SQL -->
		<section>
			<h3 class="text-xs font-semibold text-neutral-500 uppercase tracking-wider mb-3">Generated SQL</h3>
			<div class="bg-neutral-950 border border-neutral-800 rounded-md overflow-hidden">
				<div class="flex items-center justify-between px-3 py-1.5 bg-neutral-800 border-b border-neutral-700">
					<span class="text-xs text-neutral-400">query.sql</span>
					<button class="text-neutral-400 hover:text-white"><svg class="w-3.5 h-3.5" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M8 16H6a2 2 0 01-2-2V6a2 2 0 012-2h8a2 2 0 012 2v2m-6 12h8a2 2 0 002-2v-8a2 2 0 00-2-2h-8a2 2 0 00-2 2v8a2 2 0 002 2z"></path></svg></button>
				</div>
				<pre class="p-3 text-xs text-emerald-400 font-mono overflow-x-auto"><code>{currentSql}</code></pre>
			</div>
		</section>

		<!-- Query Results -->
		<section>
			<h3 class="text-xs font-semibold text-neutral-500 uppercase tracking-wider mb-3">Execution Result</h3>
			<div class="bg-neutral-950 border border-neutral-800 rounded-md overflow-hidden">
				<div class="px-3 py-1.5 bg-neutral-800 border-b border-neutral-700 flex justify-between items-center">
					<span class="text-xs text-neutral-400">Data Sample</span>
					<span class="text-xs text-emerald-500">{queryResults.length} rows</span>
				</div>
				<div class="overflow-x-auto">
					<table class="w-full text-left border-collapse">
						<thead>
							<tr class="border-b border-neutral-800">
								{#each Object.keys(queryResults[0] || {}) as key}
									<th class="p-2 text-xs font-medium text-neutral-400 whitespace-nowrap">{key}</th>
								{/each}
							</tr>
						</thead>
						<tbody>
							{#each queryResults as row}
								<tr class="border-b border-neutral-800/50 hover:bg-neutral-800/50">
									{#each Object.values(row) as val}
										<td class="p-2 text-xs text-neutral-300 font-mono whitespace-nowrap">{val}</td>
									{/each}
								</tr>
							{/each}
						</tbody>
					</table>
				</div>
			</div>
		</section>

	</div>
</aside>
{/if}
