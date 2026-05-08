<script lang="ts">
	import { page } from '$app/state';
	import { onMount } from 'svelte';
	import { marked } from 'marked';

	let fqdn = $derived(page.params.fqdn);
	let tableData = $state(null);
	let loading = $state(true);

	onMount(async () => {
		try {
			const res = await fetch(`/api/core/tables/${fqdn}`);
			const data = await res.json();
			if (data.status === 'ok') {
				tableData = data.table;
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
		
		<nav class="mb-6 flex items-center space-x-2 text-xs">
			<a href="/tables" class="text-neutral-500 hover:text-emerald-400">Tables</a>
			<span class="text-neutral-700">/</span>
			<span class="text-neutral-300">{fqdn.replace(/__DOT__/g, '.').split('.').pop()?.split('=')[1] || fqdn}</span>
		</nav>

		{#if loading}
			<div class="space-y-4 animate-pulse">
				<div class="h-10 bg-neutral-800 w-1/3 rounded-lg"></div>
				<div class="h-64 bg-neutral-800 w-full rounded-xl"></div>
			</div>
		{:else if tableData}
			<header class="mb-8">
				<h1 class="text-3xl font-bold text-white">{fqdn.replace(/__DOT__/g, '.').split('.').pop()?.split('=')[1] || fqdn}</h1>
				<p class="text-neutral-500 font-mono text-sm mt-1">{fqdn.replace(/__DOT__/g, '.')}</p>
			</header>

			<div class="grid grid-cols-1 gap-8">
				
				<!-- Columns Section -->
				<section class="bg-neutral-800 border border-neutral-700 rounded-2xl overflow-hidden shadow-sm">
					<div class="px-6 py-4 border-b border-neutral-700 bg-neutral-700/30">
						<h2 class="text-sm font-bold uppercase tracking-wider text-neutral-300">Columns & Schema</h2>
					</div>
					<div class="p-6 overflow-x-auto prose prose-invert prose-emerald max-w-none">
						{#if tableData.columns}
							{@html marked.parse(tableData.columns)} 
						{:else}
							<p class="italic text-neutral-500">No column information available.</p>
						{/if}
					</div>
				</section>

				<!-- Preview Section -->
				<section class="bg-neutral-800 border border-neutral-700 rounded-2xl overflow-hidden shadow-sm">
					<div class="px-6 py-4 border-b border-neutral-700 bg-neutral-700/30">
						<h2 class="text-sm font-bold uppercase tracking-wider text-neutral-300">Data Preview</h2>
					</div>
					<div class="p-6 overflow-x-auto prose prose-invert prose-emerald max-w-none">
						{#if tableData.preview}
							{@html marked.parse(tableData.preview)}
						{:else}
							<p class="italic text-neutral-500">No data preview available.</p>
						{/if}
					</div>
				</section>

				<!-- Profiling Section -->
				{#if tableData.profiling}
					<section class="bg-neutral-800 border border-neutral-700 rounded-2xl overflow-hidden shadow-sm">
						<div class="px-6 py-4 border-b border-neutral-700 bg-neutral-700/30">
							<h2 class="text-sm font-bold uppercase tracking-wider text-neutral-300">Data Profiling</h2>
						</div>
						<div class="p-6 overflow-x-auto prose prose-invert prose-sm max-w-none">
							{@html tableData.profiling}
						</div>
					</section>
				{/if}

			</div>
		{:else}
			<div class="text-center py-20 bg-neutral-800/30 rounded-2xl">
				<p class="text-neutral-500">Table not found or metadata missing.</p>
			</div>
		{/if}
	</div>
</div>
