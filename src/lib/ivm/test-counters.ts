import { View } from '$lib/ivm/sinks/view.js';
import { defaultComparator } from '$lib/ivm/sources/memory.js';
import { CountGroupByOperator } from '$lib/ivm/stream-operators/aggregation/count-group-by-operator.js';
import { CountOperator } from '$lib/ivm/stream-operators/aggregation/count-operator.js';
import { GroupByOperator } from '$lib/ivm/stream-operators/group-by-operator.js';
import { JoinOperator } from '$lib/ivm/stream-operators/join-operator.js';
import { LimitOperator } from '$lib/ivm/stream-operators/limit-operator.js';
import { ProjectOperator } from '$lib/ivm/stream-operators/project-operator.js';

// This is userland code.
// Think about how a query builder/interface
// will make this nicer
export function createCountView(source) {
	const countConn = source.connect();
	const count = new CountOperator(countConn, { column: 'id' });
	const countView = new View(count, defaultComparator('count'));

	return {
		subscribe: (cb) =>
			countView.subscribe((c) => {
				return cb(c[0]?.count || 0);
			}),
		materialize: () => countView.materialize()[0]?.count || 0
	};
}

export function createCountByView({ source, groupByKeys, orderBy }) {
	// ===== REACTIONS BY EMOJI =====
	const conn = source.connect();
	const group = new GroupByOperator(
		conn,
		groupByKeys,
		// this maintains internal ordering within each group,
		// most aggregation (COUNT, SUM, AVG, etc...) doesn't care about the order,
		// just that it's consistent.
		// a custom comparator might be needed for:
		// 1. composite key
		// 2. if we care about FIRST() or LAST(), or need specific ordering
		// 3. window functions for specific ordering within partitions
		defaultComparator('id')
	);
	const byCount = new CountGroupByOperator(group);

	const reactionsByEmojiView = new View(byCount, orderBy);
	return {
		subscribe: (cb) => {
			return reactionsByEmojiView.subscribe(cb);
		},
		materialize: () => reactionsByEmojiView.materialize()
	};
}

export function topBy({
	topSource,
	bySource,
	orderBy,
	groupByKeys,
	countKeyExtractor,
	joinKeyExtractor,
	limit,
	select,
	joinOrderBy
}) {
	const topConn = topSource.connect();
	const topGroup = new GroupByOperator(topConn, groupByKeys, defaultComparator('id'));
	const topCount = new CountGroupByOperator(topGroup);

	// Join with users to get names
	const byConn = bySource.connect();
	type CountResult = { userId: number; count: number };
	const join = new JoinOperator<CountResult, unknown>(
		topCount,
		byConn,
		countKeyExtractor,
		joinKeyExtractor,
		joinOrderBy
	);

	// Project to flatten the tuple
	const project = new ProjectOperator(join, {
		columns: select
	});

	// Limit to top 5
	type ReactorResult = { userId: number; userName: string; count: number };
	const topLimit = new LimitOperator<ReactorResult>(project, limit, orderBy);
	const topView = new View(topLimit, orderBy);

	return {
		subscribe: (cb) => topView.subscribe(cb),
		materialize: () => topView.materialize()
	};
}
