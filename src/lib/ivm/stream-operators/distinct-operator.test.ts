import { describe, expect, it } from 'vitest';
import { Memory } from '../sources/memory.ts';
import { View } from '../sinks/view.ts';
import { ProjectOperator } from './project-operator.ts';
import { JoinOperator } from './join-operator.ts';
import { SplitStreamOperator } from './split-stream-operator.ts';
import { DistinctOperator } from './distinct-operator.ts';

describe('distinct', () => {
	it('not a test really just a reminder tha the comparator can remove rows if not careful', () => {
		const initialData = [
			{ userId: 1, id: 1, amount: 10 },
			{ userId: 1, id: 2, amount: 20 },
			{ userId: 2, id: 3, amount: 30 },
			{ userId: 3, id: 4, amount: 40 }
		];

		const source = new Memory({ initialData, pk: 'id', schema: {} });
		const sourceComparator = (rowA, rowB) => {
			if (rowA.userId < rowB.userId) return -1;
			if (rowA.userId > rowB.userId) return 1;
			if (rowA.id < rowB.id) return -1;
			if (rowA.id > rowB.id) return 1;
			return 0;
		};
		const conn = source.connect(['id', 'asc'], sourceComparator);
		const v = new View(conn, sourceComparator);
		const results = v.materialize();
		expect(results).toStrictEqual([
			{ userId: 1, id: 1, amount: 10 },
			{ userId: 1, id: 2, amount: 20 },
			{ userId: 2, id: 3, amount: 30 },
			{ userId: 3, id: 4, amount: 40 }
		]);
		const source2 = new Memory({ initialData, pk: 'id', schema: {} });
		const sourceComparatorBAD = (rowA, rowB) => {
			if (rowA.userId < rowB.userId) return -1;
			if (rowA.userId > rowB.userId) return 1;
			return 0;
		};
		const conn2 = source2.connect(['id', 'asc'], sourceComparatorBAD);
		const v2 = new View(conn2, sourceComparatorBAD);
		const results2 = v2.materialize();
		expect(results2, 'we expect bad results from a bad comparator').toStrictEqual([
			{ userId: 1, id: 2, amount: 20 }, // <-- THIS IS BAD, MAKE SURE YOUR COMPARATOR IS RIGHT
			{ userId: 1, id: 2, amount: 20 },
			{ userId: 2, id: 3, amount: 30 },
			{ userId: 3, id: 4, amount: 40 }
		]);
	});
});

describe('DistinctOperator', () => {
	describe('initial data (pull)', () => {
		it('removes duplicates from initial data', () => {
			type Row = { userId: number; amount: number };
			const initialData: Row[] = [
				{ userId: 100, amount: 50 },
				{ userId: 100, amount: 50 }, // duplicate
				{ userId: 100, amount: 50 }, // duplicate
				{ userId: 200, amount: 75 }
			];

			const source = new Memory({ initialData, pk: 'userId', schema: {} });
			const conn = source.connect();

			const comparator = (a: Row, b: Row) => {
				if (a.userId !== b.userId) return a.userId - b.userId;
				return a.amount - b.amount;
			};

			const distinct = new DistinctOperator(conn, comparator);
			const view = new View(distinct, comparator);
			const results = view.materialize();

			expect(results).toStrictEqual([
				{ userId: 100, amount: 50 },
				{ userId: 200, amount: 75 }
			]);
		});

		it('handles empty source', () => {
			type Row = { id: number; value: string };
			const source = new Memory<Row>({ initialData: [], pk: 'id', schema: {} });
			const conn = source.connect();

			const comparator = (a: Row, b: Row) => a.id - b.id;
			const distinct = new DistinctOperator(conn, comparator);
			const view = new View(distinct, comparator);

			expect(view.materialize()).toStrictEqual([]);
		});

		it('handles all unique rows (no duplicates)', () => {
			type Row = { id: number; value: string };
			const initialData: Row[] = [
				{ id: 1, value: 'a' },
				{ id: 2, value: 'b' },
				{ id: 3, value: 'c' }
			];

			const source = new Memory({ initialData, pk: 'id', schema: {} });
			const conn = source.connect();

			const comparator = (a: Row, b: Row) => a.id - b.id;
			const distinct = new DistinctOperator(conn, comparator);
			const view = new View(distinct, comparator);

			expect(view.materialize()).toStrictEqual(initialData);
		});

		it('handles all duplicates (single unique value)', () => {
			type Row = { value: number };
			const initialData: Row[] = [
				{ value: 42 },
				{ value: 42 },
				{ value: 42 },
				{ value: 42 },
				{ value: 42 }
			];

			const source = new Memory({ initialData, pk: 'value', schema: {} });
			const conn = source.connect();

			const comparator = (a: Row, b: Row) => a.value - b.value;
			const distinct = new DistinctOperator(conn, comparator);
			const view = new View(distinct, comparator);

			expect(view.materialize()).toStrictEqual([{ value: 42 }]);
		});
	});

	describe('incremental updates (push)', () => {
		it('emits new unique rows on addition', () => {
			type Row = { id: number; value: string };
			const initialData: Row[] = [{ id: 1, value: 'a' }];

			const source = new Memory({ initialData, pk: 'id', schema: {} });
			const conn = source.connect();

			const comparator = (a: Row, b: Row) => a.id - b.id;
			const distinct = new DistinctOperator(conn, comparator);
			const view = new View(distinct, comparator);

			expect(view.materialize()).toStrictEqual([{ id: 1, value: 'a' }]);

			// Add a new unique row
			source.add({ id: 2, value: 'b' });
			expect(view.materialize()).toStrictEqual([
				{ id: 1, value: 'a' },
				{ id: 2, value: 'b' }
			]);
		});

		it('does not emit when duplicate is added', () => {
			type Order = { id: number; userId: number; amount: number };
			type Projected = { userId: number; amount: number };

			const initialData: Order[] = [
				{ id: 1, userId: 100, amount: 50 },
				{ id: 2, userId: 200, amount: 75 }
			];

			const source = new Memory({ initialData, pk: 'id', schema: {} });
			const conn = source.connect();

			// Project to remove PK
			const project = new ProjectOperator<Order, Projected>(conn, {
				columns: {
					userId: (row) => row.userId,
					amount: (row) => row.amount
				}
			});

			const comparator = (a: Projected, b: Projected) => {
				if (a.userId !== b.userId) return a.userId - b.userId;
				return a.amount - b.amount;
			};

			const distinct = new DistinctOperator(project, comparator);
			const view = new View(distinct, comparator);
			let results = view.materialize();
			view.subscribe((r) => {
				results = r;
			});
			expect(results).toStrictEqual([
				{ userId: 100, amount: 50 },
				{ userId: 200, amount: 75 }
			]);

			// Add a row that becomes a duplicate after projection
			source.add({ id: 3, userId: 100, amount: 50 });

			// Should still only have two unique rows in distinct output
			expect(results).toStrictEqual([
				{ userId: 100, amount: 50 },
				{ userId: 200, amount: 75 }
			]);
		});

		it('maintains reference counts correctly', () => {
			type Order = { id: number; userId: number; amount: number };
			type Projected = { userId: number; amount: number };

			const initialData: Order[] = [
				{ id: 1, userId: 100, amount: 50 },
				{ id: 2, userId: 100, amount: 50 }, // duplicate after projection
				{ id: 3, userId: 200, amount: 75 }
			];

			const source = new Memory({ initialData, pk: 'id', schema: {} });
			const conn = source.connect();

			// Project to remove PK - creates duplicates
			const project = new ProjectOperator<Order, Projected>(conn, {
				columns: {
					userId: (row) => row.userId,
					amount: (row) => row.amount
				}
			});

			const comparator = (a: Projected, b: Projected) => {
				if (a.userId !== b.userId) return a.userId - b.userId;
				return a.amount - b.amount;
			};

			const distinct = new DistinctOperator(project, comparator);
			const view = new View(distinct, comparator);
			let results = view.materialize();

			view.subscribe((r) => {
				results = r;
			});
			// Initial: 2 unique values (id:1 and id:2 are duplicates after projection)
			expect(results).toStrictEqual([
				{ userId: 100, amount: 50 },
				{ userId: 200, amount: 75 }
			]);

			// Add another duplicate (id:4 projects to same as id:1 and id:2)
			source.add({ id: 4, userId: 100, amount: 50 });
			expect(results).toStrictEqual([
				{ userId: 100, amount: 50 },
				{ userId: 200, amount: 75 }
			]);

			// Remove one instance (id:1) - count goes from 3 to 2
			source.remove({ id: 1, userId: 100, amount: 50 });
			expect(results).toStrictEqual([
				{ userId: 100, amount: 50 },
				{ userId: 200, amount: 75 }
			]);

			// Remove another instance (id:2) - count goes from 2 to 1
			source.remove({ id: 2, userId: 100, amount: 50 });
			expect(results).toStrictEqual([
				{ userId: 100, amount: 50 },
				{ userId: 200, amount: 75 }
			]);

			// Remove last instance (id:4) - count goes to 0, should be removed
			source.remove({ id: 4, userId: 100, amount: 50 });
			expect(results).toStrictEqual([{ userId: 200, amount: 75 }]);
		});

		it('removes row when all instances deleted', () => {
			type Row = { id: number; value: string };
			const initialData: Row[] = [
				{ id: 1, value: 'a' },
				{ id: 2, value: 'b' }
			];

			const source = new Memory({ initialData, pk: 'id', schema: {} });
			const conn = source.connect();

			const comparator = (a: Row, b: Row) => a.id - b.id;
			const distinct = new DistinctOperator(conn, comparator);
			const view = new View(distinct, comparator);
			let results = view.materialize();
			view.subscribe((r) => {
				results = r;
			});
			expect(results).toStrictEqual([
				{ id: 1, value: 'a' },
				{ id: 2, value: 'b' }
			]);

			// Remove one row
			source.remove({ id: 1, value: 'a' });
			expect(results).toStrictEqual([{ id: 2, value: 'b' }]);

			// Remove the other row
			source.remove({ id: 2, value: 'b' });
			expect(results).toStrictEqual([]);
		});

		it('handles updates (delete + add) correctly', () => {
			type Row = { id: number; value: string };
			const initialData: Row[] = [{ id: 1, value: 'old' }];

			const source = new Memory({ initialData, pk: 'id', schema: {} });
			const conn = source.connect();

			const comparator = (a: Row, b: Row) => a.id - b.id;
			const distinct = new DistinctOperator(conn, comparator);
			const view = new View(distinct, comparator);

			expect(view.materialize()).toStrictEqual([{ id: 1, value: 'old' }]);

			// Simulate UPDATE: delete old, add new
			source.remove({ id: 1, value: 'old' });
			source.add({ id: 1, value: 'new' });

			expect(view.materialize()).toStrictEqual([{ id: 1, value: 'new' }]);
		});

		it('works with view subscription', () => {
			type Row = { id: number; value: string };
			const initialData: Row[] = [{ id: 1, value: 'a' }];

			const source = new Memory({ initialData, pk: 'id', schema: {} });
			const conn = source.connect();

			const comparator = (a: Row, b: Row) => a.id - b.id;
			const distinct = new DistinctOperator(conn, comparator);
			const view = new View(distinct, comparator);

			// Materialize first to populate the view
			view.materialize();

			let results: readonly Row[] = [];
			view.subscribe((r) => {
				results = r;
			});

			// Initial state
			expect(results).toStrictEqual([{ id: 1, value: 'a' }]);

			// Add new row
			source.add({ id: 2, value: 'b' });
			expect(results).toStrictEqual([
				{ id: 1, value: 'a' },
				{ id: 2, value: 'b' }
			]);
		});
	});

	describe('integration with other operators', () => {
		it('removes duplicates created by projection', () => {
			type Order = { id: number; userId: number; amount: number };
			type Projected = { userId: number; amount: number };

			const initialData: Order[] = [
				{ id: 1, userId: 100, amount: 50 },
				{ id: 2, userId: 100, amount: 50 }, // duplicate after projection
				{ id: 3, userId: 200, amount: 75 }
			];

			const source = new Memory({ initialData, pk: 'id', schema: {} });
			const conn = source.connect();

			const project = new ProjectOperator<Order, Projected>(conn, {
				columns: {
					userId: (row) => row.userId,
					amount: (row) => row.amount
				}
			});

			const comparator = (a: Projected, b: Projected) => {
				if (a.userId !== b.userId) return a.userId - b.userId;
				return a.amount - b.amount;
			};

			const distinct = new DistinctOperator(project, comparator);
			const view = new View(distinct, comparator);

			expect(view.materialize()).toStrictEqual([
				{ userId: 100, amount: 50 },
				{ userId: 200, amount: 75 }
			]);
		});

		it('removes duplicates created by join then projection', () => {
			type User = { userId: number; name: string };
			type Order = { orderId: number; userId: number; amount: number };
			type JoinedRow = [User, Order];
			type Projected = { userId: number };

			const users: User[] = [
				{ userId: 100, name: 'Alice' },
				{ userId: 200, name: 'Bob' }
			];

			const orders: Order[] = [
				{ orderId: 1, userId: 100, amount: 50 },
				{ orderId: 2, userId: 100, amount: 75 },
				{ orderId: 3, userId: 200, amount: 100 }
			];

			const usersSource = new Memory({ initialData: users, pk: 'userId', schema: {} });
			const ordersSource = new Memory({ initialData: orders, pk: 'orderId', schema: {} });

			const join = new JoinOperator<User, Order>(
				usersSource.connect(),
				ordersSource.connect(),
				(user) => user.userId,
				(order) => order.userId,
				(a: JoinedRow, b: JoinedRow) => a[1].orderId - b[1].orderId
			);

			const project = new ProjectOperator<JoinedRow, Projected>(join, {
				columns: {
					userId: (row) => row[0].userId
				}
			});

			const comparator = (a: Projected, b: Projected) => a.userId - b.userId;
			const distinct = new DistinctOperator(project, comparator);
			const view = new View(distinct, comparator);

			expect(view.materialize()).toStrictEqual([{ userId: 100 }, { userId: 200 }]);
		});
	});
});

describe('duplicate detection', () => {
	it('creates duplicates via projection (removing PK)', () => {
		// Setup: Multiple orders with same userId and amount
		type Order = { id: number; userId: number; amount: number };
		type Projected = { userId: number; amount: number };

		const initialData: Order[] = [
			{ id: 1, userId: 100, amount: 50 },
			{ id: 2, userId: 100, amount: 50 }, // Same userId AND amount as id:1
			{ id: 3, userId: 200, amount: 75 },
			{ id: 4, userId: 100, amount: 50 } // Another duplicate of id:1
		];

		const source = new Memory({ initialData, pk: 'id', schema: {} });
		const conn = source.connect();

		// Project away the 'id' field - this creates duplicates!
		const project = new ProjectOperator<Order, Projected>(conn, {
			columns: {
				userId: (row) => row.userId,
				amount: (row) => row.amount
			}
		});

		const comparator = (a: Projected, b: Projected) => {
			if (a.userId !== b.userId) return a.userId - b.userId;
			return a.amount - b.amount;
		};

		const view = new View(project, comparator);
		const results = view.materialize();

		// We should get 4 rows, with 3 being identical
		expect(results.length).toBe(4);
		expect(results).toStrictEqual([
			{ userId: 100, amount: 50 },
			{ userId: 100, amount: 50 },
			{ userId: 100, amount: 50 },
			{ userId: 200, amount: 75 }
		]);

		// Verify we have duplicates
		const duplicates = results.filter((r) => r.userId === 100 && r.amount === 50);
		expect(duplicates.length).toBe(3);
	});

	it('creates duplicates via self-join on non-unique column', () => {
		// Setup: Orders with same userId
		type Order = { orderId: number; userId: number; amount: number };
		type JoinedRow = [Order, Order];

		const orders: Order[] = [
			{ orderId: 1, userId: 100, amount: 50 },
			{ orderId: 2, userId: 100, amount: 75 }, // Same userId
			{ orderId: 3, userId: 200, amount: 100 }
		];

		const source = new Memory({ initialData: orders, pk: 'orderId', schema: {} });
		const conn1 = source.connect();
		const conn2 = source.connect();

		const comparator1 = (a: JoinedRow, b: JoinedRow) => {
			// Compare left side first
			if (a[0].orderId !== b[0].orderId) return a[0].orderId - b[0].orderId;
			// Compare right side as tiebreaker
			return a[1].orderId - b[1].orderId;
		};

		// Self-join on userId - creates cartesian product for matching userIds
		const selfJoin = new JoinOperator<Order, Order>(
			conn1,
			conn2,
			(order) => order.userId,
			(order) => order.userId,
			comparator1
		);
		const split = new SplitStreamOperator(selfJoin);
		const b1 = split.branch();
		const view = new View(b1, comparator1);

		const results = view.materialize();

		// Should produce 5 joined rows:
		// - 4 for userId:100 (2x2 cartesian product)
		// - 1 for userId:200 (1x1)
		expect(results.length).toBe(5);
		expect(results, 'should not contain duplicates').toStrictEqual([
			[
				{ amount: 50, orderId: 1, userId: 100 },
				{ amount: 50, orderId: 1, userId: 100 }
			],
			[
				{ amount: 50, orderId: 1, userId: 100 },
				{ amount: 75, orderId: 2, userId: 100 }
			],
			[
				{ amount: 75, orderId: 2, userId: 100 },
				{ amount: 50, orderId: 1, userId: 100 }
			],
			[
				{ amount: 75, orderId: 2, userId: 100 },
				{ amount: 75, orderId: 2, userId: 100 }
			],
			[
				{ amount: 100, orderId: 3, userId: 200 },
				{ amount: 100, orderId: 3, userId: 200 }
			]
		]);

		const b2 = split.branch();
		const distinct = new DistinctOperator(b2, comparator1);
		const view2 = new View(distinct, comparator1);

		const results2 = view2.materialize();
		expect(results2, 'still should not contain duplicates').toStrictEqual([
			[
				{ amount: 50, orderId: 1, userId: 100 },
				{ amount: 50, orderId: 1, userId: 100 }
			],
			[
				{ amount: 50, orderId: 1, userId: 100 },
				{ amount: 75, orderId: 2, userId: 100 }
			],
			[
				{ amount: 75, orderId: 2, userId: 100 },
				{ amount: 50, orderId: 1, userId: 100 }
			],
			[
				{ amount: 75, orderId: 2, userId: 100 },
				{ amount: 75, orderId: 2, userId: 100 }
			],
			[
				{ amount: 100, orderId: 3, userId: 200 },
				{ amount: 100, orderId: 3, userId: 200 }
			]
		]);
	});

	it('creates duplicates via projection after join', () => {
		// Setup: Join users with orders, then project to just userId
		type User = { userId: number; name: string };
		type Order = { orderId: number; userId: number; amount: number };
		type JoinedRow = [User, Order];
		type Projected = { userId: number };

		const users: User[] = [
			{ userId: 100, name: 'Alice' },
			{ userId: 200, name: 'Bob' }
		];

		const orders: Order[] = [
			{ orderId: 1, userId: 100, amount: 50 },
			{ orderId: 2, userId: 100, amount: 75 }, // Alice has 2 orders
			{ orderId: 3, userId: 200, amount: 100 }
		];

		const usersSource = new Memory({ initialData: users, pk: 'userId', schema: {} });
		const ordersSource = new Memory({ initialData: orders, pk: 'orderId', schema: {} });

		const usersConn = usersSource.connect();
		const ordersConn = ordersSource.connect();

		// Join users with orders
		const join = new JoinOperator<User, Order>(
			usersConn,
			ordersConn,
			(user) => user.userId,
			(order) => order.userId,
			(a: JoinedRow, b: JoinedRow) => a[1].orderId - b[1].orderId
		);

		// Project to just userId - creates duplicates for users with multiple orders
		const project = new ProjectOperator<JoinedRow, Projected>(join, {
			columns: {
				userId: (row) => row[0].userId
			}
		});
		const comparator = (a: Projected, b: Projected) => a.userId - b.userId;
		const split = new SplitStreamOperator(project);
		const b1 = split.branch();
		const view = new View(b1, comparator);
		const results = view.materialize();

		// Should have 3 rows with userId:100 appearing twice
		expect(results, 'has duplicates!').toStrictEqual([
			{ userId: 100 },
			{ userId: 100 }, // Duplicate! (from Alice's 2 orders)
			{ userId: 200 }
		]);

		const b2 = split.branch();
		const distinct = new DistinctOperator(b2, comparator);
		const view2 = new View(distinct, comparator);
		const results2 = view2.materialize();

		expect(results2, 'no duplicates!').toStrictEqual([{ userId: 100 }, { userId: 200 }]);
	});
});
