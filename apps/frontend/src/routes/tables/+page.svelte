<script lang="ts">
	import { onMount } from 'svelte';
	let tables = $state([]);
	let loading = $state(true);

	onMount(async () => {
		try {
			const res = await fetch('/api/core/tables');
			const data = await res.json();
			if (data.status === 'ok') {
				tables = data.tables;
			}
		} catch (e) {
			console.error(e);
		} finally {
			loading = false;
		}
	});
</script>

<div class="flex-1 p-8 bg-neutral-900 overflow-y-auto">
	<div class="max-w-5xl mx-auto">
		<header class="mb-8">
			<h1 class="text-2xl font-bold text-neutral-100">Table Explorer</h1>
			<p class="text-neutral-500 mt-1">Browse and inspect database schemas from your project.</p>
		</header>

		{#if loading}
			<div class="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
				{#each Array(6) as _}
					<div class="h-32 bg-neutral-800 rounded-xl animate-pulse"></div>
				{/each}
			</div>
		{:else if tables.length > 0}
			<div class="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
				{#each tables as table}
					<a href="/tables/{table.fqdn}" class="group bg-neutral-800 border border-neutral-700 p-5 rounded-xl hover:bg-neutral-700/50 hover:border-emerald-500/50 transition-all shadow-sm">
						<div class="flex items-center justify-between mb-3">
							<span class="text-[10px] font-bold uppercase tracking-widest text-neutral-500 group-hover:text-emerald-400 transition-colors">{table.database}</span>
							<svg class="w-4 h-4 text-neutral-600 group-hover:text-emerald-500 transition-colors" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 5l7 7-7 7"></path></svg>
						</div>
						<h3 class="font-bold text-neutral-200 group-hover:text-white transition-colors truncate">{table.table}</h3>
						<p class="text-xs text-neutral-500 mt-1 truncate">{table.schema}</p>
					</a>
				{/each}
			</div>
		{:else}
			<div class="text-center py-20 bg-neutral-800/30 rounded-2xl border border-dashed border-neutral-700">
				<div class="text-neutral-500 mb-2">No tables found.</div>
				<p class="text-xs text-neutral-600">Make sure you have run <code class="bg-neutral-900 px-1 py-0.5 rounded text-neutral-400">ca3 sync</code> in your project folder.</p>
			</div>
		{/if}
	</div>
</div>
