<script lang="ts">
	import { appStore } from './app/app-store.ts';
	import { todosCount, allTodos } from './app/repos/todos.ts';
	import { schema } from './app/schema.ts';

	let count = $state(todosCount.execute()[0]['count(*)']);
	todosCount.subscribe(([c]) => {
		count = c['count(*)'];
	});

	let todos = $state(allTodos.execute());
	allTodos.subscribe((t) => {
		todos = t;
	});

	function addTodo() {
		appStore.commit(schema.events.todoCreated({ id: crypto.randomUUID(), text: 'groceries' }));
	}
	function toggleTodo(todo) {
		const event = todo.completed
			? schema.events.todoUncompleted({ id: todo.id })
			: schema.events.todoCompleted({ id: todo.id });
		appStore.commit(event);
	}

	function deleteTodo(todo) {
		appStore.commit(schema.events.todoDeleted({ id: todo.id }));
	}

	let isUpdating = $state(false);
	let todosToGenerate = $state(1000);
	function bulkAdd(maxMsPerFrame = 5) {
		isUpdating = true;

		// Calculate intervals for proportional distribution
		const totalItems = 1000;
		const itemsInterval = totalItems / todosToGenerate;

		let itemsProcessed = 0;

		function addNextItem() {
			// Calculate scores to determine which type is most "due"
			const itemScore = (itemsProcessed + 1) / itemsInterval;

			// Add the item type that's most behind schedule
			const id = crypto.randomUUID();
			appStore.commit(
				schema.events.todoCreated({ id: crypto.randomUUID(), text: `name ${id.slice(0, 4)}` })
			);
			itemsProcessed++;
		}

		function processBatch() {
			const startTime = performance.now();

			// Process items until we hit time budget
			while (itemsProcessed < totalItems && performance.now() - startTime < maxMsPerFrame) {
				addNextItem();
			}

			// Continue in next frame if more items remain
			if (itemsProcessed < totalItems) {
				requestAnimationFrame(processBatch);
			}
			if (itemsProcessed === totalItems) {
				isUpdating = false;
			}
		}

		requestAnimationFrame(processBatch);
	}
</script>

<h3>todos</h3>
<ul>
	<li>batch `emit`s to the database</li>
	<li>batch events to the other tabs so they can quickly update ivm</li>
</ul>
<button type="button" onclick={addTodo}>add todo</button>
<button type="button" onclick={() => bulkAdd()}>bulk add</button>
<div>todos: {count}</div>
{#each todos as todo (todo.id)}
	<div class={todo.completed ? 'done' : ''}>
		{todo.id}: {todo.text} <button type="button" onclick={() => toggleTodo(todo)}>toggle</button>
		<button type="button" onclick={() => deleteTodo(todo)}>X</button>
	</div>
{/each}

<style>
	.done {
		text-decoration: line-through;
	}
</style>
