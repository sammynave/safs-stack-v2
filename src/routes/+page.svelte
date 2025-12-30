<script lang="ts">
	import { todosCount } from './app/repos/todos.ts';

	let count = todosCount.exec();
	todosCount.subscribe((c) => (count = c));
</script>

<div>todos: {count}</div>
