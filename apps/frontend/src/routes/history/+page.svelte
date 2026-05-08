<script lang="ts">
	import { onMount } from 'svelte';

	type ChatHistoryItem = {
		id: string;
		title: string;
		updatedAt: number;
	};

	let history = $state<ChatHistoryItem[]>([]);

	onMount(async () => {
		const res = await fetch('/api/core/chats');
		const data = await res.json();
		if (data.status === 'ok') history = data.chats;
	});
</script>

<div class="flex-1 p-8 bg-neutral-900 overflow-y-auto">
	<h1 class="text-2xl font-bold text-neutral-100 mb-8">Chat History</h1>
	
	<div class="grid gap-4">
		{#each history as chat}
			<a href="/?chatId={chat.id}" class="block bg-neutral-800 border border-neutral-700 p-4 rounded-xl hover:bg-neutral-700 transition-colors">
				<div class="font-medium text-neutral-200">{chat.title}</div>
				<div class="text-xs text-neutral-500 mt-1">{new Date(chat.updatedAt * 1000).toLocaleString()}</div>
			</a>
		{:else}
			<div class="text-neutral-500 italic">No history found yet. Start a new chat!</div>
		{/each}
	</div>
</div>
