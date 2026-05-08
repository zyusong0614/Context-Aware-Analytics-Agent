<script lang="ts">
	let evals = $state([
		{ id: 1, name: 'Basic Revenue Query', status: 'pending', lastRun: null },
		{ id: 2, name: 'Customer Retention Logic', status: 'pass', lastRun: '2024-05-07' },
		{ id: 3, name: 'Complex Join Aggregation', status: 'fail', lastRun: '2024-05-07' }
	]);

	async function runEval(id: number) {
		const ev = evals.find(e => e.id === id);
		if (ev) ev.status = 'running';
		
		try {
			const res = await fetch('/api/core/evals/run', { 
				method: 'POST', 
				headers: { 'Content-Type': 'application/json' },
				body: JSON.stringify({ id }) 
			});
			const data = await res.json();
			
			if (data.status === 'ok') {
				if (ev) {
					ev.status = 'pass'; // You could further refine this by checking data.results
					ev.lastRun = new Date().toISOString().split('T')[0];
				}
			} else {
				if (ev) ev.status = 'fail';
			}
		} catch (e) {
			console.error(e);
			if (ev) ev.status = 'fail';
		}
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
				{#each evals as test}
					<tr class="hover:bg-neutral-700/30 transition-colors">
						<td class="p-4 text-sm font-medium text-neutral-200">{test.name}</td>
						<td class="p-4">
							<span class="px-2 py-1 rounded-full text-[10px] font-bold uppercase tracking-wider
								{test.status === 'pass' ? 'bg-emerald-500/10 text-emerald-500 border border-emerald-500/20' : 
								 test.status === 'fail' ? 'bg-red-500/10 text-red-500 border border-red-500/20' : 
								 test.status === 'running' ? 'bg-blue-500/10 text-blue-500 border border-blue-500/20 animate-pulse' :
								 'bg-neutral-500/10 text-neutral-500 border border-neutral-500/20'}">
								{test.status}
							</span>
						</td>
						<td class="p-4 text-xs text-neutral-500 font-mono">{test.lastRun || 'Never'}</td>
						<td class="p-4 text-right">
							<button 
								onclick={() => runEval(test.id)}
								class="text-xs bg-neutral-700 hover:bg-neutral-600 text-neutral-200 px-3 py-1.5 rounded-md transition-colors border border-neutral-600"
							>
								Run Test
							</button>
						</td>
					</tr>
				{/each}
			</tbody>
		</table>
	</div>
</div>
