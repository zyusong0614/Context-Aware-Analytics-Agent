<script lang="ts">
	import './layout.css';
	import favicon from '$lib/assets/favicon.svg';
	import { onMount } from 'svelte';
	import { page } from '$app/state';

	let { children } = $props();
	let currentPath = $derived(page.url.pathname);
	
	// App state
	let project = $state(null);
	
	onMount(async () => {
		try {
			const res = await fetch('/api/core/project');
			const data = await res.json();
			if (data.status === 'ok') {
				project = data.project;
			}
		} catch (e) {
			console.error('Failed to load project', e);
		}
	});
</script>

<svelte:head>
	<title>CA3 Analytics Workspace</title>
	<link rel="icon" href={favicon} />
</svelte:head>

<div class="flex h-screen bg-neutral-900 text-neutral-100 font-sans overflow-hidden">
	
	<!-- Left Sidebar -->
	<aside class="w-64 bg-neutral-950 border-r border-neutral-800 flex flex-col shrink-0">
		<!-- Project Info -->
		<div class="p-4 border-b border-neutral-800">
			<h2 class="text-sm font-bold tracking-wider text-neutral-400 uppercase mb-1">Current Project</h2>
			{#if project}
				<div class="font-medium text-emerald-400">{project.name}</div>
				<div class="text-xs text-neutral-500 truncate" title={project.path}>{project.path}</div>
			{:else}
				<div class="animate-pulse bg-neutral-800 h-4 w-24 rounded"></div>
			{/if}
		</div>

		<!-- Navigation -->
		<nav class="flex-1 overflow-y-auto p-4 space-y-6">
			<div>
				<h3 class="text-xs font-semibold text-neutral-500 uppercase tracking-wider mb-3">Database</h3>
				<ul class="space-y-1">
					<li>
						<a href="/tables" class="flex items-center space-x-2 px-2 py-1.5 rounded-md hover:bg-neutral-800 text-sm transition-colors {currentPath.startsWith('/tables') ? 'bg-neutral-800 text-emerald-400' : 'text-neutral-300'}">
							<svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M4 7v10c0 2.21 3.582 4 8 4s8-1.79 8-4V7M4 7c0 2.21 3.582 4 8 4s8-1.79 8-4M4 7c0-2.21 3.582-4 8-4s8 1.79 8 4m0 5c0 2.21-3.582 4-8 4s-8-1.79-8-4"></path></svg>
							<span>Table Explorer</span>
						</a>
					</li>
				</ul>
			</div>
			
			<div>
				<h3 class="text-xs font-semibold text-neutral-500 uppercase tracking-wider mb-3">Workspace</h3>
				<ul class="space-y-1">
					<li>
						<a href="/" class="flex items-center space-x-2 px-2 py-1.5 rounded-md hover:bg-neutral-800 text-sm transition-colors {currentPath === '/' ? 'bg-neutral-800 text-emerald-400 font-medium' : 'text-neutral-300'}">
							<svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M8 12h.01M12 12h.01M16 12h.01M21 12c0 4.418-4.03 8-9 8a9.863 9.863 0 01-4.255-.949L3 20l1.395-3.72C3.512 15.042 3 13.574 3 12c0-4.418 4.03-8 9-8s9 3.582 9 8z"></path></svg>
							<span>Active Chat</span>
						</a>
					</li>
					<li>
						<a href="/history" class="flex items-center space-x-2 px-2 py-1.5 rounded-md hover:bg-neutral-800 text-sm transition-colors {currentPath === '/history' ? 'bg-neutral-800 text-emerald-400' : 'text-neutral-300'}">
							<svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 8v4l3 3m6-3a9 9 0 11-18 0 9 9 0 0118 0z"></path></svg>
							<span>Chat History</span>
						</a>
					</li>
				</ul>
			</div>

			<div>
				<h3 class="text-xs font-semibold text-neutral-500 uppercase tracking-wider mb-3">Testing</h3>
				<ul class="space-y-1">
					<li>
						<a href="/evals" class="flex items-center space-x-2 px-2 py-1.5 rounded-md hover:bg-neutral-800 text-sm transition-colors {currentPath === '/evals' ? 'bg-neutral-800 text-emerald-400' : 'text-neutral-300'}">
							<svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 12l2 2 4-4m6 2a9 9 0 11-18 0 9 9 0 0118 0z"></path></svg>
							<span>Evaluations</span>
						</a>
					</li>
				</ul>
			</div>
		</nav>
	</aside>

	<!-- Main Content Area -->
	<main class="flex-1 flex min-w-0 relative">
		{@render children()}
	</main>

</div>
