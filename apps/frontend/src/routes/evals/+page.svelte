<script lang="ts">
	import { onMount } from 'svelte';
	type EvalRunResult = {
		id: string;
		passed: boolean;
		model: string;
		durationMs: number;
		message: string;
		timestamp?: string;
		checks?: Record<string, unknown>;
		generatedSql?: string | null;
		actualRows?: Array<Record<string, unknown>>;
		expectedRows?: Array<Record<string, unknown>>;
		error?: string;
	};

	type EvalCase = {
		id: string;
		question: string;
		file?: string;
		threshold?: number;
		status: 'pending' | 'running' | 'pass' | 'fail';
		lastRun: string | null;
		lastResult?: EvalRunResult | null;
		details?: EvalRunResult | null;
		error?: string | null;
	};

	let evals = $state<EvalCase[]>([]);
	let loading = $state(true);

	onMount(async () => {
		const res = await fetch('/api/core/evals');
		const data = await res.json();
		if (data.status === 'ok') {
			evals = data.evals.map((e: any) => ({
				...e,
				status: e.lastResult ? (e.lastResult.passed ? 'pass' : 'fail') : 'pending',
				lastRun: e.lastResult?.timestamp ? e.lastResult.timestamp.split('T')[0] : null,
				details: e.lastResult ?? null,
				error: null,
			}));
		}
		loading = false;
	});

	async function runEval(id: string) {
		const ev = evals.find(e => e.id === id);
		if (ev) {
			ev.status = 'running';
			ev.error = null;
		}
		
		try {
			const res = await fetch('/api/core/evals/run', { 
				method: 'POST', 
				headers: { 'Content-Type': 'application/json' },
				body: JSON.stringify({ id }) 
			});
			const data = await res.json();
			
			if (data.status === 'ok') {
				if (ev) {
					ev.status = data.result?.passed ? 'pass' : 'fail';
					ev.lastRun = data.result?.timestamp ? data.result.timestamp.split('T')[0] : new Date().toISOString().split('T')[0];
					ev.details = data.result;
				}
			} else {
				if (ev) {
					ev.status = 'fail';
					ev.error = data.message || 'Evaluation failed';
				}
			}
		} catch (e) {
			console.error(e);
			if (ev) {
				ev.status = 'fail';
				ev.error = e instanceof Error ? e.message : 'Evaluation failed';
			}
		}
	}

	function sampleRows(rows: Array<Record<string, unknown>> | undefined) {
		return JSON.stringify((rows ?? []).slice(0, 5), null, 2);
	}
</script>

<div class="flex-1 p-8 bg-neutral-900 overflow-y-auto">
	<h1 class="text-2xl font-bold text-neutral-100 mb-8">Evaluations</h1>
	
	<div class="bg-neutral-800 border border-neutral-700 rounded-xl overflow-hidden shadow-sm">
		<table class="w-full text-left border-collapse">
			<thead>
				<tr class="bg-neutral-950 border-b border-neutral-700">
					<th class="p-4 text-xs font-semibold text-neutral-400 uppercase">Test Case</th>
					<th class="p-4 text-xs font-semibold text-neutral-400 uppercase">Status</th>
					<th class="p-4 text-xs font-semibold text-neutral-400 uppercase">Last Run</th>
					<th class="p-4 text-xs font-semibold text-neutral-400 uppercase text-right">Actions</th>
				</tr>
			</thead>
			<tbody class="divide-y divide-neutral-700/50">
				{#if loading}
					<tr><td colspan="4" class="p-8 text-center text-neutral-500">Loading tests...</td></tr>
				{:else}
					{#each evals as test}
						<tr class="hover:bg-neutral-700/30 transition-colors">
							<td class="p-4 align-top">
								<div class="text-sm font-medium text-neutral-200">{test.question}</div>
								<div class="text-[10px] text-neutral-500 font-mono mt-1">{test.id}</div>
								{#if test.details || test.error}
									<div class="mt-4 space-y-3">
										{#if test.error}
											<div class="text-xs text-red-300 bg-red-950/30 border border-red-500/20 rounded-md p-3">{test.error}</div>
										{/if}
										{#if test.details}
											<div class="text-xs text-neutral-400">
												<span class="text-neutral-500">Model:</span> {test.details.model}
												<span class="text-neutral-600 mx-2">/</span>
												<span class="text-neutral-500">Duration:</span> {test.details.durationMs}ms
											</div>
											{#if test.details.error}
												<div class="text-xs text-red-300 bg-red-950/30 border border-red-500/20 rounded-md p-3">{test.details.error}</div>
											{/if}
											<div class="text-xs text-neutral-300 bg-neutral-950/70 border border-neutral-700 rounded-md p-3">
												<div class="text-neutral-500 uppercase font-semibold mb-2">Message</div>
												{test.details.message}
											</div>
											{#if test.details.generatedSql}
												<div class="bg-neutral-950 border border-neutral-700 rounded-md overflow-hidden">
													<div class="px-3 py-2 text-[10px] uppercase font-semibold text-neutral-500 border-b border-neutral-800">Generated SQL</div>
													<pre class="p-3 text-xs text-emerald-300 overflow-x-auto">{test.details.generatedSql}</pre>
												</div>
											{/if}
											{#if test.details.checks}
												<div class="bg-neutral-950 border border-neutral-700 rounded-md overflow-hidden">
													<div class="px-3 py-2 text-[10px] uppercase font-semibold text-neutral-500 border-b border-neutral-800">Checks</div>
													<pre class="p-3 text-xs text-neutral-300 overflow-x-auto">{JSON.stringify(test.details.checks, null, 2)}</pre>
												</div>
											{/if}
											<div class="grid md:grid-cols-2 gap-3">
												<div class="bg-neutral-950 border border-neutral-700 rounded-md overflow-hidden">
													<div class="px-3 py-2 text-[10px] uppercase font-semibold text-neutral-500 border-b border-neutral-800">Expected Rows</div>
													<pre class="p-3 text-xs text-neutral-300 overflow-x-auto">{sampleRows(test.details.expectedRows)}</pre>
												</div>
												<div class="bg-neutral-950 border border-neutral-700 rounded-md overflow-hidden">
													<div class="px-3 py-2 text-[10px] uppercase font-semibold text-neutral-500 border-b border-neutral-800">Actual Rows</div>
													<pre class="p-3 text-xs text-neutral-300 overflow-x-auto">{sampleRows(test.details.actualRows)}</pre>
												</div>
											</div>
										{/if}
									</div>
								{/if}
							</td>
							<td class="p-4 align-top">
								<span class="px-2 py-1 rounded-full text-[10px] font-bold uppercase tracking-wider
									{test.status === 'pass' ? 'bg-emerald-500/10 text-emerald-500 border border-emerald-500/20' : 
									 test.status === 'fail' ? 'bg-red-500/10 text-red-500 border border-red-500/20' : 
									 test.status === 'running' ? 'bg-blue-500/10 text-blue-500 border border-blue-500/20 animate-pulse' :
									 'bg-neutral-500/10 text-neutral-500 border border-neutral-500/20'}">
									{test.status}
								</span>
							</td>
						<td class="p-4 text-xs text-neutral-500 font-mono align-top">{test.lastRun || 'Never'}</td>
						<td class="p-4 text-right align-top">
							<button 
								onclick={() => runEval(test.id)}
								disabled={test.status === 'running'}
								class="text-xs bg-neutral-700 hover:bg-neutral-600 text-neutral-200 px-3 py-1.5 rounded-md transition-colors border border-neutral-600"
							>
								{test.status === 'running' ? 'Running...' : 'Run Test'}
							</button>
						</td>
					</tr>
				{/each}
			{/if}
			</tbody>
		</table>
	</div>
</div>
