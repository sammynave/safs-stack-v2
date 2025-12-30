import { describe, expect, it } from 'vitest';
import { defaultComparator, Memory } from './sources/memory.ts';
import { FilterOperator } from './stream-operators/filter-operator.ts';
import { JoinOperator } from './stream-operators/join-operator.ts';
import { View } from './sinks/view.ts';
import { LimitOperator } from './stream-operators/limit-operator.ts';
import { SpyOperator } from './stream-operators/spy-operator.ts';
import { GroupByOperator } from './stream-operators/group-by-operator.ts';
import { SumOperator } from './stream-operators/aggregation/sum-operator.ts';

describe('ivm', () => {
	it('all operators work together - materialized', () => {
		const users = new Memory({
			initialData: [
				{ id: 1, name: 'Sammy', age: 43 },
				{ id: 2, name: 'Fern', age: 7 },
				{ id: 3, name: 'Del', age: 4 },
				{ id: 4, name: 'Emily', age: 42 }
			],
			pk: 'id',
			schema: { id: Number, name: String }
		});
		const orders = new Memory({
			initialData: [
				{ id: 1, userId: 1, amount: 10 },
				{ id: 2, userId: 1, amount: 7 },
				{ id: 3, userId: 4, amount: 20 },
				{ id: 4, userId: 4, amount: 100 },
				{ id: 5, userId: 3, amount: 2 },
				{ id: 6, userId: 4, amount: 99 },
				{ id: 7, userId: 1, amount: 25 }
			],
			pk: 'id',
			schema: { id: Number, userId: Number, amount: Number }
		});
		const userConn = users.connect(['age', 'asc'], (user, user2) => user.age - user2.age);
		const userData = [...userConn.pull()];
		expect(userData, 'userData to be correct').toStrictEqual([
			[{ age: 4, id: 3, name: 'Del' }, 1],
			[{ age: 7, id: 2, name: 'Fern' }, 1],
			[{ age: 42, id: 4, name: 'Emily' }, 1],
			[{ age: 43, id: 1, name: 'Sammy' }, 1]
		]);
		const orderConn = orders.connect(['id', 'asc'], (order, order2) => order.id - order2.id);
		const orderData = [...orderConn.pull()];
		expect(orderData, 'orderData to be correct').toStrictEqual([
			[{ amount: 10, id: 1, userId: 1 }, 1],
			[{ amount: 7, id: 2, userId: 1 }, 1],
			[{ amount: 20, id: 3, userId: 4 }, 1],
			[{ amount: 100, id: 4, userId: 4 }, 1],
			[{ amount: 2, id: 5, userId: 3 }, 1],
			[{ amount: 99, id: 6, userId: 4 }, 1],
			[{ amount: 25, id: 7, userId: 1 }, 1]
		]);
		const ageOver40 = new FilterOperator(userConn, (user) => user.age > 40);
		const amountOver9 = new FilterOperator(orderConn, (order) => order.amount > 9);

		const sortByAgeThenAmount = (a: unknown, b: unknown): 0 | 1 | -1 => {
			const rowA = a;
			const rowB = b;
			const userA = rowA[0];
			const userB = rowB[0];
			const orderA = rowA[1];
			const orderB = rowB[1];

			// Primary sort by age DESC (higher age first)
			if (userA.age > userB.age) return -1;
			if (userA.age < userB.age) return 1;

			// Secondary sort by amount DESC (higher amount first)
			if (orderA.amount > orderB.amount) return -1;
			if (orderA.amount < orderB.amount) return 1;

			// Tertiary sort by order id for stability (ASC)
			if (orderA.id < orderB.id) return -1;
			if (orderA.id > orderB.id) return 1;

			return 0;
		};

		const userOrders = new JoinOperator(
			ageOver40,
			amountOver9,
			(user) => user.id,
			(order) => order.userId,
			sortByAgeThenAmount
		);
		const limit = new LimitOperator(userOrders, 4, sortByAgeThenAmount);
		const limitedView = new View(limit, sortByAgeThenAmount);
		expect(limitedView.materialize(), 'limited').toStrictEqual([
			[
				{ age: 43, id: 1, name: 'Sammy' },
				{ amount: 25, id: 7, userId: 1 }
			],
			[
				{ age: 43, id: 1, name: 'Sammy' },
				{ amount: 10, id: 1, userId: 1 }
			],
			[
				{ age: 42, id: 4, name: 'Emily' },
				{ amount: 100, id: 4, userId: 4 }
			],
			[
				{ age: 42, id: 4, name: 'Emily' },
				{ amount: 99, id: 6, userId: 4 }
			]
		]);

		users.add({ id: 5, name: 'Dood', age: 100 });
		orders.add({ id: 8, userId: 5, amount: 200 });

		expect(limitedView.materialize(), 'limited after add').toStrictEqual([
			[
				{ age: 100, id: 5, name: 'Dood' },
				{ amount: 200, id: 8, userId: 5 }
			],
			[
				{ age: 43, id: 1, name: 'Sammy' },
				{ amount: 25, id: 7, userId: 1 }
			],
			[
				{ age: 43, id: 1, name: 'Sammy' },
				{ amount: 10, id: 1, userId: 1 }
			],
			[
				{ age: 42, id: 4, name: 'Emily' },
				{ amount: 100, id: 4, userId: 4 }
			]
		]);
	});

	it('all operators work together - subscriptions', () => {
		const users = new Memory({
			initialData: [
				{ id: 1, name: 'Sammy', age: 43 },
				{ id: 2, name: 'Fern', age: 7 },
				{ id: 3, name: 'Del', age: 4 },
				{ id: 4, name: 'Emily', age: 42 }
			],
			pk: 'id',
			schema: { id: Number, name: String }
		});
		const orders = new Memory({
			initialData: [
				{ id: 1, userId: 1, amount: 10 },
				{ id: 2, userId: 1, amount: 7 },
				{ id: 3, userId: 4, amount: 20 },
				{ id: 4, userId: 4, amount: 100 },
				{ id: 5, userId: 3, amount: 2 },
				{ id: 6, userId: 4, amount: 99 },
				{ id: 7, userId: 1, amount: 25 }
			],
			pk: 'id',
			schema: { id: Number, userId: Number, amount: Number }
		});
		const userConn = users.connect(['age', 'asc'], (user, user2) => user.age - user2.age);
		const userData = [...userConn.pull()];
		expect(userData, 'userData to be correct').toStrictEqual([
			[{ age: 4, id: 3, name: 'Del' }, 1],
			[{ age: 7, id: 2, name: 'Fern' }, 1],
			[{ age: 42, id: 4, name: 'Emily' }, 1],
			[{ age: 43, id: 1, name: 'Sammy' }, 1]
		]);
		const orderConn = orders.connect(['id', 'asc'], (order, order2) => order.id - order2.id);
		const orderData = [...orderConn.pull()];
		expect(orderData, 'orderData to be correct').toStrictEqual([
			[{ amount: 10, id: 1, userId: 1 }, 1],
			[{ amount: 7, id: 2, userId: 1 }, 1],
			[{ amount: 20, id: 3, userId: 4 }, 1],
			[{ amount: 100, id: 4, userId: 4 }, 1],
			[{ amount: 2, id: 5, userId: 3 }, 1],
			[{ amount: 99, id: 6, userId: 4 }, 1],
			[{ amount: 25, id: 7, userId: 1 }, 1]
		]);
		const ageOver40 = new FilterOperator(userConn, (user) => user.age > 40);
		const amountOver9 = new FilterOperator(orderConn, (order) => order.amount > 9);

		const sortByAgeThenAmount = (a: unknown, b: unknown): 0 | 1 | -1 => {
			const rowA = a;
			const rowB = b;
			const userA = rowA[0];
			const userB = rowB[0];
			const orderA = rowA[1];
			const orderB = rowB[1];

			// Primary sort by age DESC (higher age first)
			if (userA.age > userB.age) return -1;
			if (userA.age < userB.age) return 1;

			// Secondary sort by amount DESC (higher amount first)
			if (orderA.amount > orderB.amount) return -1;
			if (orderA.amount < orderB.amount) return 1;

			// Tertiary sort by order id for stability (ASC)
			if (orderA.id < orderB.id) return -1;
			if (orderA.id > orderB.id) return 1;

			return 0;
		};

		const userOrders = new JoinOperator(
			ageOver40,
			amountOver9,
			(user) => user.id,
			(order) => order.userId,
			sortByAgeThenAmount
		);

		const limit = new LimitOperator(userOrders, 4, sortByAgeThenAmount);
		const limitedView = new View(limit, sortByAgeThenAmount);

		let viewLimitedSub = limitedView.materialize();
		limitedView.subscribe((x) => {
			viewLimitedSub = x;
		});

		users.add({ id: 5, name: 'Dood', age: 100 });
		orders.add({ id: 8, userId: 5, amount: 200 });

		expect(viewLimitedSub, 'limited sub after add').toStrictEqual([
			[
				{ age: 100, id: 5, name: 'Dood' },
				{ amount: 200, id: 8, userId: 5 }
			],
			[
				{ age: 43, id: 1, name: 'Sammy' },
				{ amount: 25, id: 7, userId: 1 }
			],
			[
				{ age: 43, id: 1, name: 'Sammy' },
				{ amount: 10, id: 1, userId: 1 }
			],
			[
				{ age: 42, id: 4, name: 'Emily' },
				{ amount: 100, id: 4, userId: 4 }
			]
		]);
	});
});

describe('FilterOperator - Efficiency Tests', () => {
	it('should process only delta rows, not entire dataset', () => {
		type Row = { id: number; value: number };
		const DATA_LENGTH = 1000;
		// Setup: 1000 initial rows
		const initialData: Row[] = Array.from({ length: DATA_LENGTH }, (_, i) => ({
			id: i,
			value: i * 10
		}));
		expect(initialData.length).toBe(DATA_LENGTH);
		const source = new Memory({ initialData, pk: 'id', schema: null });

		const conn = source.connect();

		const sourceSpy = new SpyOperator(conn);
		expect(sourceSpy.metrics, 'source spy lazy after initilization').toStrictEqual({
			pushCalls: 0,
			pushRowCounts: [],
			emptyPushes: 0,
			totalRowsPushed: 0,
			pullCalls: 0,
			totalRowsPulled: 0,
			pullIterations: []
		});
		const filter = new FilterOperator(sourceSpy, (row: Row) => row.value < 500);
		expect(sourceSpy.metrics, 'source spy lazy after filter').toStrictEqual({
			pushCalls: 0,
			pushRowCounts: [],
			emptyPushes: 0,
			totalRowsPushed: 0,
			pullCalls: 0,
			totalRowsPulled: 0,
			pullIterations: []
		});
		const filterSpy = new SpyOperator(filter);
		expect(sourceSpy.metrics, 'source spy lazy after filter spy').toStrictEqual({
			pushCalls: 0,
			pushRowCounts: [],
			emptyPushes: 0,
			totalRowsPushed: 0,
			pullCalls: 0,
			totalRowsPulled: 0,
			pullIterations: []
		});
		expect(filterSpy.metrics, 'filter spy lazy after filter spy').toStrictEqual({
			pushCalls: 0,
			pushRowCounts: [],
			emptyPushes: 0,
			totalRowsPushed: 0,
			pullCalls: 0,
			totalRowsPulled: 0,
			pullIterations: []
		});
		const view = new View(filterSpy, defaultComparator('id'));
		expect(sourceSpy.metrics, 'source spy still lazy after view').toStrictEqual({
			pushCalls: 0,
			pushRowCounts: [],
			emptyPushes: 0,
			totalRowsPushed: 0,
			pullCalls: 0,
			totalRowsPulled: 0,
			pullIterations: []
		});
		expect(filterSpy.metrics, 'filter spy still lazy after view').toStrictEqual({
			pushCalls: 0,
			pushRowCounts: [],
			emptyPushes: 0,
			totalRowsPushed: 0,
			pullCalls: 0,
			totalRowsPulled: 0,
			pullIterations: []
		});
		// Initial state: should have filtered ~50 rows (value > 500)
		const initialResults = view.materialize();
		expect(
			sourceSpy.metrics,
			'sourceSpy should have a pull with all rows after materialize'
		).toStrictEqual({
			pushCalls: 0,
			pushRowCounts: [],
			emptyPushes: 0,
			totalRowsPushed: 0,
			pullCalls: 1,
			totalRowsPulled: DATA_LENGTH,
			pullIterations: [DATA_LENGTH]
		});
		expect(
			filterSpy.metrics,
			'filterSpy should have a pull with JUST THE ROWS FILTERED rows after materialize'
		).toStrictEqual({
			pushCalls: 0,
			pushRowCounts: [],
			emptyPushes: 0,
			totalRowsPushed: 0,
			pullCalls: 1,
			totalRowsPulled: 50,
			pullIterations: [50]
		});
		expect(initialResults.length, 'initial results are correct').toBe(50);
		// TEST: Add ONE new row that fails filter
		source.add({ id: 2000, value: 1000 });
		expect(
			sourceSpy.metrics,
			'sourceSpy should have a push with just the new value row after add'
		).toStrictEqual({
			pushCalls: 1,
			pushRowCounts: [1],
			emptyPushes: 0,
			totalRowsPushed: 1,
			pullCalls: 1,
			totalRowsPulled: DATA_LENGTH,
			pullIterations: [DATA_LENGTH]
		});
		expect(
			filterSpy.metrics,
			'filterSpy should still have 0 pushes since the add is filtered out'
		).toStrictEqual({
			pushCalls: 0,
			pushRowCounts: [],
			emptyPushes: 0,
			totalRowsPushed: 0,
			pullCalls: 1,
			totalRowsPulled: 50,
			pullIterations: [50]
		});
		// TEST: Add ONE new row that passes filter
		source.add({ id: 2001, value: 10 });

		expect(
			sourceSpy.metrics,
			'sourceSpy should have another push with just the new value row after add'
		).toStrictEqual({
			pushCalls: 2,
			pushRowCounts: [1, 1],
			emptyPushes: 0,
			totalRowsPushed: 2,
			pullCalls: 1,
			totalRowsPulled: DATA_LENGTH,
			pullIterations: [DATA_LENGTH]
		});
		expect(
			filterSpy.metrics,
			'filterSpy should now have 1 push since the add is passes the filter'
		).toStrictEqual({
			pushCalls: 1,
			pushRowCounts: [1],
			emptyPushes: 0,
			totalRowsPushed: 1,
			pullCalls: 1,
			totalRowsPulled: 50,
			pullIterations: [50]
		});
		// // Result should have the new row
		const results = view.materialize();
		expect(results.length, 'should have only 1 new row').toBe(initialResults.length + 1);
		expect(
			results.find((r) => r.id === 2000),
			'the row that fails the filter is not in the view'
		).toBeUndefined();
		expect(
			results.find((r) => r.id === 2001),
			'the row that passes the filter is in the view'
		).toBeDefined();
	});

	it('should handle removal efficiently', () => {
		type Row = { id: number; value: number };

		const initialData: Row[] = [
			{ id: 1, value: 100 },
			{ id: 2, value: 200 },
			{ id: 3, value: 300 }
		];

		const source = new Memory({ initialData, pk: 'id', schema: null });
		const conn = source.connect();
		const sourceSpy = new SpyOperator(conn);
		const filter = new FilterOperator(sourceSpy, (row: Row) => row.value > 150);
		const filterSpy = new SpyOperator(filter);
		const view = new View(filterSpy, defaultComparator('id'));

		expect(sourceSpy.metrics, 'source spy lazy after initilization').toStrictEqual({
			pushCalls: 0,
			pushRowCounts: [],
			emptyPushes: 0,
			totalRowsPushed: 0,
			pullCalls: 0,
			totalRowsPulled: 0,
			pullIterations: []
		});
		expect(filterSpy.metrics, 'filter spy lazy after initilization').toStrictEqual({
			pushCalls: 0,
			pushRowCounts: [],
			emptyPushes: 0,
			totalRowsPushed: 0,
			pullCalls: 0,
			totalRowsPulled: 0,
			pullIterations: []
		});

		// Initial: 2 rows pass (200, 300)
		expect(view.materialize().length).toBe(2);
		expect(sourceSpy.metrics, 'source spy pulls all').toStrictEqual({
			pushCalls: 0,
			pushRowCounts: [],
			emptyPushes: 0,
			totalRowsPushed: 0,
			pullCalls: 1,
			totalRowsPulled: 3,
			pullIterations: [3]
		});
		expect(filterSpy.metrics, 'filter spy lazy after initilization').toStrictEqual({
			pushCalls: 0,
			pushRowCounts: [],
			emptyPushes: 0,
			totalRowsPushed: 0,
			pullCalls: 1,
			totalRowsPulled: 2,
			pullIterations: [2]
		});

		// Remove row that does not pass filter
		source.remove({ id: 1, value: 100 });

		expect(sourceSpy.metrics, 'source spy gets the push').toStrictEqual({
			pushCalls: 1,
			pushRowCounts: [1],
			emptyPushes: 0,
			totalRowsPushed: 1,
			pullCalls: 1,
			totalRowsPulled: 3,
			pullIterations: [3]
		});
		expect(
			filterSpy.metrics,
			'filter spy does not get the push since the remove was filtered out'
		).toStrictEqual({
			pushCalls: 0,
			pushRowCounts: [],
			emptyPushes: 0,
			totalRowsPushed: 0,
			pullCalls: 1,
			totalRowsPulled: 2,
			pullIterations: [2]
		});
		// Remove row that passes filter
		source.remove({ id: 2, value: 200 });

		expect(sourceSpy.metrics, 'source spy gets the next removal push').toStrictEqual({
			pushCalls: 2,
			pushRowCounts: [1, 1],
			emptyPushes: 0,
			totalRowsPushed: 2,
			pullCalls: 1,
			totalRowsPulled: 3,
			pullIterations: [3]
		});
		expect(filterSpy.metrics, 'filter spy gets the push this time').toStrictEqual({
			pushCalls: 1,
			pushRowCounts: [1],
			emptyPushes: 0,
			totalRowsPushed: 1,
			pullCalls: 1,
			totalRowsPulled: 2,
			pullIterations: [2]
		});

		// Result should have 1 row now
		const results = view.materialize();
		expect(results.length, 'there is only 1 row in the final view').toBe(1);
		expect(results).toStrictEqual([{ id: 3, value: 300 }]);
	});

	it('should demonstrate efficiency vs naive approach', () => {
		type Row = { id: number; value: number };

		const DATA_SIZE = 1000000;
		// Large dataset
		const initialData: Row[] = Array.from({ length: DATA_SIZE }, (_, i) => ({
			id: i,
			value: i
		}));

		const source = new Memory({ initialData, pk: 'id', schema: null });
		const conn = source.connect();
		const sourceSpy = new SpyOperator(conn);
		const filter = new FilterOperator(sourceSpy, (row: Row) => row.value < 100);
		const filterSpy = new SpyOperator(filter);
		const view = new View(filterSpy, defaultComparator('id'));

		expect(sourceSpy.metrics, 'source spy lazy after initilization').toStrictEqual({
			pushCalls: 0,
			pushRowCounts: [],
			emptyPushes: 0,
			totalRowsPushed: 0,
			pullCalls: 0,
			totalRowsPulled: 0,
			pullIterations: []
		});
		expect(filterSpy.metrics, 'filter spy lazy after initilization').toStrictEqual({
			pushCalls: 0,
			pushRowCounts: [],
			emptyPushes: 0,
			totalRowsPushed: 0,
			pullCalls: 0,
			totalRowsPulled: 0,
			pullIterations: []
		});

		// Add 10 rows
		for (let i = DATA_SIZE; i < DATA_SIZE + 10; i++) {
			source.add({ id: i, value: i });
		}

		expect(sourceSpy.metrics, 'pushes only, no pulls (i.e. full scans)').toStrictEqual({
			pushCalls: 10,
			pushRowCounts: [1, 1, 1, 1, 1, 1, 1, 1, 1, 1],
			emptyPushes: 0,
			totalRowsPushed: 10,
			pullCalls: 0,
			totalRowsPulled: 0,
			pullIterations: []
		});
		expect(
			filterSpy.metrics,
			'filter spy has nothing to do since the new rows failed the filter'
		).toStrictEqual({
			pushCalls: 0,
			pushRowCounts: [],
			emptyPushes: 0,
			totalRowsPushed: 0,
			pullCalls: 0,
			totalRowsPulled: 0,
			pullIterations: []
		});
		const results = view.materialize();
		expect(results[0]).toStrictEqual({ id: 0, value: 0 });
		expect(results[99]).toStrictEqual({ id: 99, value: 99 });
		expect(results[100]).toBeUndefined();
		expect(sourceSpy.metrics, 'pushes only, no pulls (i.e. full scans)').toStrictEqual({
			pushCalls: 10,
			pushRowCounts: [1, 1, 1, 1, 1, 1, 1, 1, 1, 1],
			emptyPushes: 0,
			totalRowsPushed: 10,
			pullCalls: 1,
			totalRowsPulled: DATA_SIZE + 10,
			pullIterations: [DATA_SIZE + 10]
		});
		expect(
			filterSpy.metrics,
			'filter spy has nothing to do since the new rows failed the filter'
		).toStrictEqual({
			pushCalls: 0,
			pushRowCounts: [],
			emptyPushes: 0,
			totalRowsPushed: 0,
			pullCalls: 1,
			totalRowsPulled: 100,
			pullIterations: [100]
		});
		const totalRowsPulled = sourceSpy.metrics.totalRowsPulled + filterSpy.metrics.totalRowsPulled;
		const totalRowsPushed = sourceSpy.metrics.totalRowsPushed + filterSpy.metrics.totalRowsPushed;
		const percentFaster = (totalRowsPulled / totalRowsPushed - 1) * 100;
		console.log(
			`ivm is ${new Intl.NumberFormat('en-US').format(percentFaster)}% faster than materializing`
		);
		expect(
			percentFaster,
			`processing pushes is 10 million (actual: ${new Intl.NumberFormat('en-US').format(percentFaster)}%) percent faster than querying entire set`
		).toBe(10001000);
	});
});

describe('Pull Optimization: Early Termination Analysis', () => {
	const comparator = (a: any, b: any) => {
		if (a.id < b.id) return -1;
		if (a.id > b.id) return 1;
		return 0;
	};

	describe('Stateless Operators (Should optimize)', () => {
		it('Memory → Limit(2) pulls only ~2 rows', () => {
			// Create 1000 rows
			const data = Array.from({ length: 1000 }, (_, i) => ({
				id: i,
				value: i * 10
			}));

			const source = new Memory({ initialData: data, pk: 'id', schema: null });
			const conn = source.connect(['id', 'asc'], comparator);

			// Spy on source
			const spy1 = new SpyOperator(conn);

			// Add limit
			const limit = new LimitOperator(spy1, 2, comparator);

			// Spy on limit output
			const spy2 = new SpyOperator(limit);

			// Consume the chain
			const results = [...spy2.pull()];

			// Assertions
			expect(results.length).toBe(2);
			expect(results[0][0]).toEqual({ id: 0, value: 0 });
			expect(results[1][0]).toEqual({ id: 1, value: 10 });

			expect(spy1.metrics.totalRowsPulled).toBe(2); // Early termination works!
			expect(spy2.metrics.totalRowsPulled).toBe(2);
		});

		it('Memory → Filter → Limit(2) pulls only rows until 2 match', () => {
			// Create 1000 rows
			const data = Array.from({ length: 1000 }, (_, i) => ({
				id: i,
				value: i
			}));

			const source = new Memory({ initialData: data, pk: 'id', schema: null });
			const conn = source.connect(['id', 'asc'], comparator);

			// Spy on source
			const spy1 = new SpyOperator(conn);

			// Filter: only even values
			const filter = new FilterOperator(spy1, (row) => row.value % 2 === 0);

			// Spy on filter output
			const spy2 = new SpyOperator(filter);

			// Limit to 2
			const limit = new LimitOperator(spy2, 2, comparator);

			// Spy on limit output
			const spy3 = new SpyOperator(limit);

			// Consume the chain
			const results = [...spy3.pull()];

			// Assertions
			expect(results.length).toBe(2);
			expect(results[0][0]).toEqual({ id: 0, value: 0 });
			expect(results[1][0]).toEqual({ id: 2, value: 2 });

			expect(spy1.metrics.totalRowsPulled).toBeLessThanOrEqual(4); // Early termination!
			expect(spy2.metrics.totalRowsPulled).toBe(2); // Filter passed 2 rows
			expect(spy3.metrics.totalRowsPulled).toBe(2); // Limit passed 2 rows
		});

		it('Memory → Filter(selective) → Limit(2) pulls more rows', () => {
			// Create 1000 rows
			const data = Array.from({ length: 1000 }, (_, i) => ({
				id: i,
				value: i
			}));

			const source = new Memory({ initialData: data, pk: 'id', schema: null });
			const conn = source.connect(['id', 'asc'], comparator);

			// Spy on source
			const spy1 = new SpyOperator(conn);

			// Filter: only values divisible by 100 (very selective!)
			const filter = new FilterOperator(spy1, (row) => row.value % 100 === 0);

			// Spy on filter output
			const spy2 = new SpyOperator(filter);

			// Limit to 2
			const limit = new LimitOperator(spy2, 2, comparator);

			// Spy on limit output
			const spy3 = new SpyOperator(limit);

			// Consume the chain
			const results = [...spy3.pull()];

			// Assertions
			expect(results.length).toBe(2);
			expect(results[0][0]).toEqual({ id: 0, value: 0 });
			expect(results[1][0]).toEqual({ id: 100, value: 100 });

			expect(spy1.metrics.totalRowsPulled).toBeLessThanOrEqual(201); // Early termination!
			expect(spy2.metrics.totalRowsPulled).toBe(2);
			expect(spy3.metrics.totalRowsPulled).toBe(2);
		});
	});

	describe('Stateful Operators (Cannot optimize)', () => {
		it('Memory → GroupBy → Limit(2) pulls ALL 1000 rows', () => {
			// Create 1000 rows with 10 groups
			const data = Array.from({ length: 1000 }, (_, i) => ({
				id: i,
				userId: i % 10, // 10 different groups
				value: i
			}));

			const source = new Memory({ initialData: data, pk: 'id', schema: null });
			const conn = source.connect(['id', 'asc'], comparator);

			// Spy on source
			const spy1 = new SpyOperator(conn);

			// GroupBy userId
			const rowComparator = (a: any, b: any) => a.id - b.id;
			const groupBy = new GroupByOperator(spy1, ['userId'], rowComparator);

			// Spy on groupBy output
			const spy2 = new SpyOperator(groupBy);

			// Limit to 2 groups
			const groupComparator = (a: any, b: any) => a.keyValues.userId - b.keyValues.userId;
			const limit = new LimitOperator(spy2, 2, groupComparator);

			// Spy on limit output
			const spy3 = new SpyOperator(limit);

			// Consume the chain
			const results = [...spy3.pull()];

			// Assertions
			expect(results.length).toBe(2); // Only 2 groups

			expect(spy1.metrics.totalRowsPulled).toBe(1000); // ALL data pulled!
			expect(spy2.metrics.totalRowsPulled).toBe(2); // 10 groups created
			expect(spy3.metrics.totalRowsPulled).toBe(2); // Limit to 2 groups
		});

		it('Memory → Sum → Limit(1) pulls ALL 1000 rows', () => {
			// Create 1000 rows
			const data = Array.from({ length: 1000 }, (_, i) => ({
				id: i,
				amount: i
			}));

			const source = new Memory({ initialData: data, pk: 'id', schema: null });
			const conn = source.connect(['id', 'asc'], comparator);

			// Spy on source
			const spy1 = new SpyOperator(conn);

			// Sum operator
			const sum = new SumOperator(spy1, { column: 'amount' });

			// Spy on sum output
			const spy2 = new SpyOperator(sum);

			// Limit to 1 (pointless, but tests the behavior)
			const sumComparator = (a: any, b: any) => 0;
			const limit = new LimitOperator(spy2, 1, sumComparator);

			// Spy on limit output
			const spy3 = new SpyOperator(limit);

			// Consume the chain
			const results = [...spy3.pull()];

			// Assertions
			expect(results.length).toBe(1);
			expect(results[0][0]).toEqual({ sum: 499500 }); // Sum of 0..999

			expect(spy1.metrics.totalRowsPulled).toBe(1000); // ALL data pulled!
			expect(spy2.metrics.totalRowsPulled).toBe(1); // Sum produces 1 result
			expect(spy3.metrics.totalRowsPulled).toBe(1); // Limit passes it through
		});

		it('Memory1 → Join ← Memory2 → Limit(2) pulls ALL rows from both sides', () => {
			// Create users
			const users = Array.from({ length: 100 }, (_, i) => ({
				userId: i,
				name: `User${i}`
			}));

			// Create orders (multiple per user)
			const orders = Array.from({ length: 100 }, (_, i) => ({
				orderId: i,
				userId: i % 10, // 10 users with orders
				amount: i * 10
			}));

			const usersSource = new Memory({ initialData: users, pk: 'userId', schema: null });
			const ordersSource = new Memory({ initialData: orders, pk: 'orderId', schema: null });

			const usersConn = usersSource.connect();
			const ordersConn = ordersSource.connect();

			// Spy on both sources
			const spyUsers = new SpyOperator(usersConn);
			const spyOrders = new SpyOperator(ordersConn);

			// Join
			const resultComparator = (a: any, b: any) => {
				const orderA = a[1];
				const orderB = b[1];
				return orderA.orderId - orderB.orderId;
			};

			const join = new JoinOperator(
				spyUsers,
				spyOrders,
				(user) => user.userId,
				(order) => order.userId,
				resultComparator
			);

			// Spy on join output
			const spyJoin = new SpyOperator(join);

			// Limit to 2
			const limit = new LimitOperator(spyJoin, 2, resultComparator);

			// Spy on limit output
			const spyLimit = new SpyOperator(limit);

			// Consume the chain
			const results = [...spyLimit.pull()];

			// Assertions
			expect(results.length).toBe(2);

			expect(spyUsers.metrics.totalRowsPulled).toBe(100); // ALL users pulled!
			expect(spyOrders.metrics.totalRowsPulled).toBe(100); // ALL orders pulled!
			expect(spyLimit.metrics.totalRowsPulled).toBe(2); // Limit to 2 results
		});
	});

	describe('Mixed Chains', () => {
		it('Memory → Filter → GroupBy → Limit: GroupBy blocks optimization', () => {
			const data = Array.from({ length: 1000 }, (_, i) => ({
				id: i,
				userId: i % 10,
				value: i
			}));

			const source = new Memory({ initialData: data, pk: 'id', schema: null });
			const conn = source.connect(['id', 'asc'], comparator);

			const spy1 = new SpyOperator(conn);
			const filter = new FilterOperator(spy1, (row) => row.value % 2 === 0);
			const spy2 = new SpyOperator(filter);
			const rowComparator = (a: any, b: any) => a.id - b.id;
			const groupBy = new GroupByOperator(spy2, ['userId'], rowComparator);
			const spy3 = new SpyOperator(groupBy);
			const groupComparator = (a: any, b: any) => a.keyValues.userId - b.keyValues.userId;
			const limit = new LimitOperator(spy3, 2, groupComparator);
			const spy4 = new SpyOperator(limit);
			const v = new View(spy4, comparator);
			const results = v.materialize();

			expect(spy1.metrics.totalRowsPulled).toBe(1000); // ALL rows pulled!
			expect(spy2.metrics.totalRowsPulled).toBe(500); // Filter passed 500
			expect(spy3.metrics.totalRowsPulled).toBe(2); // 10 groups
			expect(spy4.metrics.totalRowsPulled).toBe(2); // Limit to 2
			expect(results.length).toBe(2);
		});
	});

	describe('Push-based Updates (View.subscribe)', () => {
		it('Memory → Filter → Limit(2) with subscribe: initial pull optimizes', () => {
			// Create 1000 rows
			const data = Array.from({ length: 1000 }, (_, i) => ({
				id: i,
				value: i
			}));

			const source = new Memory({ initialData: data, pk: 'id', schema: null });
			const conn = source.connect(['id', 'asc'], comparator);

			// Spy on source
			const spy1 = new SpyOperator(conn);

			// Filter: only even values
			const filter = new FilterOperator(spy1, (row) => row.value % 2 === 0);

			// Spy on filter output
			const spy2 = new SpyOperator(filter);

			// Limit to 2
			const limit = new LimitOperator(spy2, 2, comparator);

			// Spy on limit output
			const spy3 = new SpyOperator(limit);

			// Create view
			const view = new View(spy3, comparator);

			// Initial materialize FIRST
			const initialData = view.materialize();

			// THEN subscribe (so we capture the current state)
			let subscriptionCallCount = 0;
			let latestData: any[] = [...initialData]; // Initialize with current state

			view.subscribe((data) => {
				subscriptionCallCount++;
				latestData = [...data]; // Copy the array since it's readonly
			});

			expect(initialData.length).toBe(2);
			expect(initialData[0]).toEqual({ id: 0, value: 0 });
			expect(initialData[1]).toEqual({ id: 2, value: 2 });

			expect(spy1.metrics.totalRowsPulled).toBeLessThanOrEqual(4); // Early termination!
			expect(spy2.metrics.totalRowsPulled).toBe(2);
			expect(spy3.metrics.totalRowsPulled).toBe(2);
			expect(subscriptionCallCount).toBe(1); // Called once on subscribe

			// Reset metrics for push phase
			spy1.resetMetrics();
			spy2.resetMetrics();
			spy3.resetMetrics();

			// Add a row with high value - won't make top-2
			source.add({ id: 1000, value: 1000 });

			// ✅ Verify push propagated through filter but NOT through limit
			expect(spy1.metrics.pushCalls, 'pushed to spy1').toBe(1);
			expect(spy1.metrics.totalRowsPushed, 'total rows spy1').toBe(1);
			expect(spy2.metrics.pushCalls, 'pushed to spy2').toBe(1);
			expect(spy2.metrics.totalRowsPushed, 'total rows spy2').toBe(1); // Filter passes it
			expect(spy3.metrics.pushCalls, 'pushed to spy3').toBe(1);
			expect(spy3.metrics.totalRowsPushed, 'total rows spy3').toBe(0); // ✅ Limit rejects it!
			expect(subscriptionCallCount).toBe(1); // NOT called again (no change)

			// ✅ Verify view was NOT updated (row was rejected)
			expect(latestData.length).toBe(2); // Still 2 rows
			expect(latestData[0]).toEqual({ id: 0, value: 0 });
			expect(latestData[1]).toEqual({ id: 2, value: 2 });

			// Reset metrics
			spy1.resetMetrics();
			spy2.resetMetrics();
			spy3.resetMetrics();

			source.remove({ id: 0, value: 0 });

			// ✅ Verify removal propagated and triggered refill
			expect(spy1.metrics.pushCalls).toBe(1);
			expect(spy1.metrics.totalRowsPushed).toBe(1); // 1 row removed
			expect(spy2.metrics.pushCalls).toBe(1);
			expect(spy2.metrics.totalRowsPushed).toBe(1); // Filter passes removal
			expect(spy3.metrics.pushCalls).toBe(1);
			expect(spy3.metrics.totalRowsPushed).toBe(2); // 2 rows: -1 (remove) +1 (refill)
			expect(subscriptionCallCount).toBe(2); // Called again on update

			// ✅ Verify view was updated with refilled row
			expect(latestData.length).toBe(2); // Still 2 rows (limit)
			expect(latestData[0]).toEqual({ id: 2, value: 2 }); // Was second, now first
			expect(latestData[1]).toEqual({ id: 4, value: 4 }); // Refilled from source
		});

		it('Memory → GroupBy → Limit(2) with subscribe: shows two-phase optimization', () => {
			// Create 1000 rows with 10 groups
			const data = Array.from({ length: 1000 }, (_, i) => ({
				id: i,
				userId: i % 10,
				value: i
			}));

			const source = new Memory({ initialData: data, pk: 'id', schema: null });
			const conn = source.connect(['id', 'asc'], comparator);

			// Spy on source
			const spy1 = new SpyOperator(conn);

			// GroupBy userId
			const rowComparator = (a: any, b: any) => a.id - b.id;
			const groupBy = new GroupByOperator(spy1, ['userId'], rowComparator);

			// Spy on groupBy output
			const spy2 = new SpyOperator(groupBy);

			// Limit to 2 groups
			const groupComparator = (a: any, b: any) => a.keyValues.userId - b.keyValues.userId;
			const limit = new LimitOperator(spy2, 2, groupComparator);

			// Spy on limit output
			const spy3 = new SpyOperator(limit);

			// Create view and subscribe (use spy3, not limit directly)
			const view = new View(spy3, groupComparator);

			// Initial materialize FIRST
			const initialData = view.materialize();

			// THEN subscribe (so we capture the current state)
			let subscriptionCallCount = 0;
			let latestData: any[] = [...initialData]; // Initialize with current state

			view.subscribe((data) => {
				subscriptionCallCount++;
				latestData = [...data]; // Copy the array
			});

			expect(initialData.length).toBe(2); // Only 2 groups

			// ❌ Input phase: Must pull ALL 1000 rows
			expect(spy1.metrics.totalRowsPulled).toBe(1000);

			// ✅ Output phase: Only yields 2 groups (Limit stops it)
			expect(spy2.metrics.totalRowsPulled).toBe(2);
			expect(spy3.metrics.totalRowsPulled).toBe(2);

			expect(subscriptionCallCount).toBe(1);

			// Reset metrics
			spy1.resetMetrics();
			spy2.resetMetrics();
			spy3.resetMetrics();

			// Add to userId 0 (first group)
			source.add({ id: 1000, userId: 0, value: 1000 });

			// ✅ Verify push propagated through all operators
			expect(spy1.metrics.pushCalls).toBe(1);
			expect(spy2.metrics.pushCalls).toBe(1);
			expect(spy3.metrics.pushCalls).toBe(1);
			expect(subscriptionCallCount).toBe(2);

			// After the push, the view should still have data (though it may be recomputed)
			expect(latestData.length).toBeGreaterThan(0);
			// The first group should have been updated
			const firstGroup = latestData[0];
			expect(firstGroup.keyValues.userId).toBeDefined();
			expect(firstGroup.rows.length).toBeGreaterThan(0);
		});
	});
});
