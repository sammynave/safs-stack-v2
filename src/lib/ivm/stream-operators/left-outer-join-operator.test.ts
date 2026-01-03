import { describe, expect, it } from 'vitest';
import { defaultComparator, Memory } from '../sources/memory.ts';
import { JoinOperator } from './join-operator.ts';
import { LogSink } from './log-operator.ts';
import { View } from '../sinks/view.ts';
import { LeftOuterJoinOperator } from './left-outer-join-operator.ts';

describe('left-outer-join-operator', () => {
	it('returns all left rows with null for unmatched right rows', () => {
		type User = { userId: number; name: string };
		type Order = { orderId: number; userId: number; amount: number };
		type JoinedRow = [User, Order | null];

		const users: User[] = [
			{ userId: 1, name: 'Alice' },
			{ userId: 2, name: 'Bob' },
			{ userId: 3, name: 'Charlie' } // No orders!
		];

		const orders: Order[] = [
			{ orderId: 101, userId: 1, amount: 50 },
			{ orderId: 102, userId: 2, amount: 100 }
			// Note: No orders for userId 3
		];

		const usersSource = new Memory({ initialData: users, pk: 'userId', schema: {} });
		const ordersSource = new Memory({ initialData: orders, pk: 'orderId', schema: {} });

		// Comparator that handles null values (sorts nulls last)
		const resultComparator = (a: JoinedRow, b: JoinedRow): number => {
			// Compare by userId first
			if (a[0].userId !== b[0].userId) return a[0].userId - b[0].userId;

			// Handle null on right side
			if (a[1] === null && b[1] === null) return 0;
			if (a[1] === null) return 1; // nulls sort last
			if (b[1] === null) return -1;

			return a[1].orderId - b[1].orderId;
		};

		const leftOuter = new LeftOuterJoinOperator(
			usersSource.connect(),
			ordersSource.connect(),
			(user) => user.userId,
			(order) => order.userId,
			resultComparator
		);

		const view = new View(leftOuter, resultComparator);
		const results = view.materialize();

		// Expected: ALL users, with null for Charlie who has no orders
		expect(results).toStrictEqual([
			[
				{ userId: 1, name: 'Alice' },
				{ orderId: 101, userId: 1, amount: 50 }
			],
			[
				{ userId: 2, name: 'Bob' },
				{ orderId: 102, userId: 2, amount: 100 }
			],
			[{ userId: 3, name: 'Charlie' }, null] // ← KEY DIFFERENCE from inner join!
		]);
	});

	it('transitions from unmatched to matched when right row is added', () => {
		type User = { userId: number; name: string };
		type Order = { orderId: number; userId: number; amount: number };
		type JoinedRow = [User, Order | null];

		// Start with user but no orders
		const users: User[] = [{ userId: 1, name: 'Alice' }];
		const orders: Order[] = [];

		const usersSource = new Memory({ initialData: users, pk: 'userId', schema: {} });
		const ordersSource = new Memory({ initialData: orders, pk: 'orderId', schema: {} });

		const resultComparator = (a: JoinedRow, b: JoinedRow): number => {
			if (a[0].userId !== b[0].userId) return a[0].userId - b[0].userId;
			if (a[1] === null && b[1] === null) return 0;
			if (a[1] === null) return 1;
			if (b[1] === null) return -1;
			return a[1].orderId - b[1].orderId;
		};

		const leftOuter = new LeftOuterJoinOperator(
			usersSource.connect(),
			ordersSource.connect(),
			(user) => user.userId,
			(order) => order.userId,
			resultComparator
		);

		const view = new View(leftOuter, resultComparator);
		let results = view.materialize();

		view.subscribe((r) => {
			results = r;
		});

		// Initial: User with no orders → [Alice, null]
		expect(results).toStrictEqual([[{ userId: 1, name: 'Alice' }, null]]);

		// Add an order for Alice
		ordersSource.add({ orderId: 101, userId: 1, amount: 50 });

		// After: Should emit TWO changes:
		// 1. DELETE [Alice, null]
		// 2. ADD [Alice, order101]
		expect(results).toStrictEqual([
			[
				{ userId: 1, name: 'Alice' },
				{ orderId: 101, userId: 1, amount: 50 }
			]
		]);

		// Add another order for Alice
		ordersSource.add({ orderId: 102, userId: 1, amount: 75 });

		// Now Alice has TWO orders (no null anymore)
		expect(results).toStrictEqual([
			[
				{ userId: 1, name: 'Alice' },
				{ orderId: 101, userId: 1, amount: 50 }
			],
			[
				{ userId: 1, name: 'Alice' },
				{ orderId: 102, userId: 1, amount: 75 }
			]
		]);

		// Remove first order
		ordersSource.remove({ orderId: 101, userId: 1, amount: 50 });

		// Still has one order (still no null)
		expect(results).toStrictEqual([
			[
				{ userId: 1, name: 'Alice' },
				{ orderId: 102, userId: 1, amount: 75 }
			]
		]);

		// Remove last order
		ordersSource.remove({ orderId: 102, userId: 1, amount: 75 });

		// Back to unmatched → [Alice, null]
		expect(results).toStrictEqual([[{ userId: 1, name: 'Alice' }, null]]);
	});

	it('handles deletions of matched and unmatched left rows', () => {
		type User = { userId: number; name: string };
		type Order = { orderId: number; userId: number; amount: number };
		type JoinedRow = [User, Order | null];

		const users: User[] = [
			{ userId: 1, name: 'Alice' },
			{ userId: 2, name: 'Bob' },
			{ userId: 3, name: 'Charlie' }
		];

		const orders: Order[] = [
			{ orderId: 101, userId: 1, amount: 50 },
			{ orderId: 102, userId: 1, amount: 30 }
			// Bob and Charlie have no orders
		];

		const usersSource = new Memory({ initialData: users, pk: 'userId', schema: {} });
		const ordersSource = new Memory({ initialData: orders, pk: 'orderId', schema: {} });

		const resultComparator = (a: JoinedRow, b: JoinedRow): number => {
			if (a[0].userId !== b[0].userId) return a[0].userId - b[0].userId;
			if (a[1] === null && b[1] === null) return 0;
			if (a[1] === null) return 1;
			if (b[1] === null) return -1;
			return a[1].orderId - b[1].orderId;
		};

		const leftOuter = new LeftOuterJoinOperator(
			usersSource.connect(),
			ordersSource.connect(),
			(user) => user.userId,
			(order) => order.userId,
			resultComparator
		);

		const view = new View(leftOuter, resultComparator);

		// Initial state: Alice has 2 orders, Bob and Charlie have null
		const initialExpected = [
			[
				{ userId: 1, name: 'Alice' },
				{ orderId: 101, userId: 1, amount: 50 }
			],
			[
				{ userId: 1, name: 'Alice' },
				{ orderId: 102, userId: 1, amount: 30 }
			],
			[{ userId: 2, name: 'Bob' }, null],
			[{ userId: 3, name: 'Charlie' }, null]
		];
		expect(view.materialize()).toStrictEqual(initialExpected);

		// Test 1: Delete unmatched left row (Charlie with null)
		usersSource.remove({ userId: 3, name: 'Charlie' });

		const afterCharlieDeleteExpected = [
			[
				{ userId: 1, name: 'Alice' },
				{ orderId: 101, userId: 1, amount: 50 }
			],
			[
				{ userId: 1, name: 'Alice' },
				{ orderId: 102, userId: 1, amount: 30 }
			],
			[{ userId: 2, name: 'Bob' }, null]
		];
		expect(view.materialize()).toStrictEqual(afterCharlieDeleteExpected);

		// Test 2: Delete matched left row (Alice with 2 orders)
		// Should remove BOTH [Alice, order101] and [Alice, order102]
		usersSource.remove({ userId: 1, name: 'Alice' });

		const afterAliceDeleteExpected = [[{ userId: 2, name: 'Bob' }, null]];
		expect(view.materialize()).toStrictEqual(afterAliceDeleteExpected);

		// Test 3: Delete last unmatched left row
		usersSource.remove({ userId: 2, name: 'Bob' });

		expect(view.materialize()).toStrictEqual([]);

		// Test 4: Verify orphaned orders don't appear (no left row to join with)
		// Orders 101 and 102 still exist in ordersSource but have no matching user
		expect(view.materialize()).toStrictEqual([]);
	});

	it('handles deletions of right rows transitioning to null', () => {
		type User = { userId: number; name: string };
		type Order = { orderId: number; userId: number; amount: number };
		type JoinedRow = [User, Order | null];

		const users: User[] = [
			{ userId: 1, name: 'Alice' },
			{ userId: 2, name: 'Bob' }
		];

		const orders: Order[] = [
			{ orderId: 101, userId: 1, amount: 50 },
			{ orderId: 102, userId: 2, amount: 100 },
			{ orderId: 103, userId: 2, amount: 200 }
		];

		const usersSource = new Memory({ initialData: users, pk: 'userId', schema: {} });
		const ordersSource = new Memory({ initialData: orders, pk: 'orderId', schema: {} });

		const resultComparator = (a: JoinedRow, b: JoinedRow): number => {
			if (a[0].userId !== b[0].userId) return a[0].userId - b[0].userId;
			if (a[1] === null && b[1] === null) return 0;
			if (a[1] === null) return 1;
			if (b[1] === null) return -1;
			return a[1].orderId - b[1].orderId;
		};

		const leftOuter = new LeftOuterJoinOperator(
			usersSource.connect(),
			ordersSource.connect(),
			(user) => user.userId,
			(order) => order.userId,
			resultComparator
		);

		const view = new View(leftOuter, resultComparator);

		// Initial: Alice has 1 order, Bob has 2 orders
		const initialExpected = [
			[
				{ userId: 1, name: 'Alice' },
				{ orderId: 101, userId: 1, amount: 50 }
			],
			[
				{ userId: 2, name: 'Bob' },
				{ orderId: 102, userId: 2, amount: 100 }
			],
			[
				{ userId: 2, name: 'Bob' },
				{ orderId: 103, userId: 2, amount: 200 }
			]
		];
		expect(view.materialize()).toStrictEqual(initialExpected);

		// Test 1: Delete Alice's only order → should transition to [Alice, null]
		ordersSource.remove({ orderId: 101, userId: 1, amount: 50 });

		const afterAliceOrderDeleteExpected = [
			[{ userId: 1, name: 'Alice' }, null],
			[
				{ userId: 2, name: 'Bob' },
				{ orderId: 102, userId: 2, amount: 100 }
			],
			[
				{ userId: 2, name: 'Bob' },
				{ orderId: 103, userId: 2, amount: 200 }
			]
		];
		expect(view.materialize()).toStrictEqual(afterAliceOrderDeleteExpected);

		// Test 2: Delete one of Bob's orders → still has one order (no null yet)
		ordersSource.remove({ orderId: 102, userId: 2, amount: 100 });

		const afterBobFirstOrderDeleteExpected = [
			[{ userId: 1, name: 'Alice' }, null],
			[
				{ userId: 2, name: 'Bob' },
				{ orderId: 103, userId: 2, amount: 200 }
			]
		];
		expect(view.materialize()).toStrictEqual(afterBobFirstOrderDeleteExpected);

		// Test 3: Delete Bob's last order → should transition to [Bob, null]
		ordersSource.remove({ orderId: 103, userId: 2, amount: 200 });

		const afterBobLastOrderDeleteExpected = [
			[{ userId: 1, name: 'Alice' }, null],
			[{ userId: 2, name: 'Bob' }, null]
		];
		expect(view.materialize()).toStrictEqual(afterBobLastOrderDeleteExpected);
	});

	it('handles updates as delete+add pairs', () => {
		type User = { userId: number; name: string };
		type Order = { orderId: number; userId: number; amount: number };
		type JoinedRow = [User, Order | null];

		const users: User[] = [
			{ userId: 1, name: 'Alice' },
			{ userId: 2, name: 'Bob' }
		];

		const orders: Order[] = [
			{ orderId: 101, userId: 1, amount: 50 }
			// Bob has no orders initially
		];

		const usersSource = new Memory({ initialData: users, pk: 'userId', schema: {} });
		const ordersSource = new Memory({ initialData: orders, pk: 'orderId', schema: {} });

		const resultComparator = (a: JoinedRow, b: JoinedRow): number => {
			if (a[0].userId !== b[0].userId) return a[0].userId - b[0].userId;
			if (a[1] === null && b[1] === null) return 0;
			if (a[1] === null) return 1;
			if (b[1] === null) return -1;
			return a[1].orderId - b[1].orderId;
		};

		const leftOuter = new LeftOuterJoinOperator(
			usersSource.connect(),
			ordersSource.connect(),
			(user) => user.userId,
			(order) => order.userId,
			resultComparator
		);

		const view = new View(leftOuter, resultComparator);

		// Initial: Alice has order, Bob has null
		const initialExpected = [
			[
				{ userId: 1, name: 'Alice' },
				{ orderId: 101, userId: 1, amount: 50 }
			],
			[{ userId: 2, name: 'Bob' }, null]
		];
		expect(view.materialize()).toStrictEqual(initialExpected);

		// Test 1: Update order amount (non-key field)
		ordersSource.remove({ orderId: 101, userId: 1, amount: 50 });
		ordersSource.add({ orderId: 101, userId: 1, amount: 75 });

		const afterAmountUpdateExpected = [
			[
				{ userId: 1, name: 'Alice' },
				{ orderId: 101, userId: 1, amount: 75 }
			],
			[{ userId: 2, name: 'Bob' }, null]
		];
		expect(view.materialize()).toStrictEqual(afterAmountUpdateExpected);

		// Test 2: Update user name while matched (non-key field)
		usersSource.remove({ userId: 1, name: 'Alice' });
		usersSource.add({ userId: 1, name: 'Alicia' });

		const afterNameUpdateExpected = [
			[
				{ userId: 1, name: 'Alicia' },
				{ orderId: 101, userId: 1, amount: 75 }
			],
			[{ userId: 2, name: 'Bob' }, null]
		];
		expect(view.materialize()).toStrictEqual(afterNameUpdateExpected);

		// Test 3: Update user name while unmatched (has null)
		usersSource.remove({ userId: 2, name: 'Bob' });
		usersSource.add({ userId: 2, name: 'Robert' });

		const afterUnmatchedNameUpdateExpected = [
			[
				{ userId: 1, name: 'Alicia' },
				{ orderId: 101, userId: 1, amount: 75 }
			],
			[{ userId: 2, name: 'Robert' }, null]
		];
		expect(view.materialize()).toStrictEqual(afterUnmatchedNameUpdateExpected);

		// Test 4: Update order to change join key (reassign to different user)
		ordersSource.remove({ orderId: 101, userId: 1, amount: 75 });
		ordersSource.add({ orderId: 101, userId: 2, amount: 75 });

		const afterKeyChangeExpected = [
			[{ userId: 1, name: 'Alicia' }, null], // Alice now unmatched
			[
				{ userId: 2, name: 'Robert' },
				{ orderId: 101, userId: 2, amount: 75 }
			] // Robert now matched
		];
		expect(view.materialize()).toStrictEqual(afterKeyChangeExpected);

		// Test 5: Update user ID (changes join key on left side)
		usersSource.remove({ userId: 2, name: 'Robert' });
		usersSource.add({ userId: 3, name: 'Robert' });

		const afterUserKeyChangeExpected = [
			[{ userId: 1, name: 'Alicia' }, null],
			[{ userId: 3, name: 'Robert' }, null] // Order 101 still has userId: 2, no match
		];
		expect(view.materialize()).toStrictEqual(afterUserKeyChangeExpected);
	});

	it('handles empty left source', () => {
		type User = { userId: number; name: string };
		type Order = { orderId: number; userId: number; amount: number };
		type JoinedRow = [User, Order | null];

		const users: User[] = [];
		const orders: Order[] = [
			{ orderId: 101, userId: 1, amount: 50 },
			{ orderId: 102, userId: 2, amount: 100 }
		];

		const usersSource = new Memory({ initialData: users, pk: 'userId', schema: {} });
		const ordersSource = new Memory({ initialData: orders, pk: 'orderId', schema: {} });

		const resultComparator = (a: JoinedRow, b: JoinedRow): number => {
			if (a[0].userId !== b[0].userId) return a[0].userId - b[0].userId;
			if (a[1] === null && b[1] === null) return 0;
			if (a[1] === null) return 1;
			if (b[1] === null) return -1;
			return a[1].orderId - b[1].orderId;
		};

		const leftOuter = new LeftOuterJoinOperator(
			usersSource.connect(),
			ordersSource.connect(),
			(user) => user.userId,
			(order) => order.userId,
			resultComparator
		);

		const view = new View(leftOuter, resultComparator);

		// Empty left source = no results (left outer join returns left rows only)
		expect(view.materialize()).toStrictEqual([]);

		// Add a user
		usersSource.add({ userId: 1, name: 'Alice' });

		// Now should have [Alice, order101]
		const afterUserAddExpected = [
			[
				{ userId: 1, name: 'Alice' },
				{ orderId: 101, userId: 1, amount: 50 }
			]
		];
		expect(view.materialize()).toStrictEqual(afterUserAddExpected);
	});

	it('handles empty right source', () => {
		type User = { userId: number; name: string };
		type Order = { orderId: number; userId: number; amount: number };
		type JoinedRow = [User, Order | null];

		const users: User[] = [
			{ userId: 1, name: 'Alice' },
			{ userId: 2, name: 'Bob' }
		];
		const orders: Order[] = [];

		const usersSource = new Memory({ initialData: users, pk: 'userId', schema: {} });
		const ordersSource = new Memory({ initialData: orders, pk: 'orderId', schema: {} });

		const resultComparator = (a: JoinedRow, b: JoinedRow): number => {
			if (a[0].userId !== b[0].userId) return a[0].userId - b[0].userId;
			if (a[1] === null && b[1] === null) return 0;
			if (a[1] === null) return 1;
			if (b[1] === null) return -1;
			return a[1].orderId - b[1].orderId;
		};

		const leftOuter = new LeftOuterJoinOperator(
			usersSource.connect(),
			ordersSource.connect(),
			(user) => user.userId,
			(order) => order.userId,
			resultComparator
		);

		const view = new View(leftOuter, resultComparator);

		// All users with null (no orders)
		const initialExpected = [
			[{ userId: 1, name: 'Alice' }, null],
			[{ userId: 2, name: 'Bob' }, null]
		];
		expect(view.materialize()).toStrictEqual(initialExpected);

		// Add an order for Alice
		ordersSource.add({ orderId: 101, userId: 1, amount: 50 });

		const afterOrderAddExpected = [
			[
				{ userId: 1, name: 'Alice' },
				{ orderId: 101, userId: 1, amount: 50 }
			],
			[{ userId: 2, name: 'Bob' }, null]
		];
		expect(view.materialize()).toStrictEqual(afterOrderAddExpected);
	});

	it('handles both sources empty', () => {
		type User = { userId: number; name: string };
		type Order = { orderId: number; userId: number; amount: number };
		type JoinedRow = [User, Order | null];

		const users: User[] = [];
		const orders: Order[] = [];

		const usersSource = new Memory({ initialData: users, pk: 'userId', schema: {} });
		const ordersSource = new Memory({ initialData: orders, pk: 'orderId', schema: {} });

		const resultComparator = (a: JoinedRow, b: JoinedRow): number => {
			if (a[0].userId !== b[0].userId) return a[0].userId - b[0].userId;
			if (a[1] === null && b[1] === null) return 0;
			if (a[1] === null) return 1;
			if (b[1] === null) return -1;
			return a[1].orderId - b[1].orderId;
		};

		const leftOuter = new LeftOuterJoinOperator(
			usersSource.connect(),
			ordersSource.connect(),
			(user) => user.userId,
			(order) => order.userId,
			resultComparator
		);

		const view = new View(leftOuter, resultComparator);

		// Both empty = no results
		expect(view.materialize()).toStrictEqual([]);

		// Add a user (no orders yet)
		usersSource.add({ userId: 1, name: 'Alice' });

		expect(view.materialize()).toStrictEqual([[{ userId: 1, name: 'Alice' }, null]]);

		// Add an order
		ordersSource.add({ orderId: 101, userId: 1, amount: 50 });

		expect(view.materialize()).toStrictEqual([
			[
				{ userId: 1, name: 'Alice' },
				{ orderId: 101, userId: 1, amount: 50 }
			]
		]);
	});

	it('handles multiple left rows with same join key', () => {
		type User = { userId: number; name: string; role: string };
		type Order = { orderId: number; userId: number; amount: number };
		type JoinedRow = [User, Order | null];

		// Two users with same userId but different roles (edge case)
		const users: User[] = [
			{ userId: 1, name: 'Alice', role: 'admin' },
			{ userId: 1, name: 'Alice', role: 'user' }
		];

		const orders: Order[] = [{ orderId: 101, userId: 1, amount: 50 }];

		const usersSource = new Memory({ initialData: users, pk: 'role', schema: {} });
		const ordersSource = new Memory({ initialData: orders, pk: 'orderId', schema: {} });

		const resultComparator = (a: JoinedRow, b: JoinedRow): number => {
			if (a[0].userId !== b[0].userId) return a[0].userId - b[0].userId;
			if (a[0].role !== b[0].role) return a[0].role.localeCompare(b[0].role);
			if (a[1] === null && b[1] === null) return 0;
			if (a[1] === null) return 1;
			if (b[1] === null) return -1;
			return a[1].orderId - b[1].orderId;
		};

		const leftOuter = new LeftOuterJoinOperator(
			usersSource.connect(),
			ordersSource.connect(),
			(user) => user.userId,
			(order) => order.userId,
			resultComparator
		);

		const view = new View(leftOuter, resultComparator);

		// Both Alice rows should join with the same order
		const initialExpected = [
			[
				{ userId: 1, name: 'Alice', role: 'admin' },
				{ orderId: 101, userId: 1, amount: 50 }
			],
			[
				{ userId: 1, name: 'Alice', role: 'user' },
				{ orderId: 101, userId: 1, amount: 50 }
			]
		];
		expect(view.materialize()).toStrictEqual(initialExpected);

		// Remove the order - both should transition to null
		ordersSource.remove({ orderId: 101, userId: 1, amount: 50 });

		const afterOrderRemoveExpected = [
			[{ userId: 1, name: 'Alice', role: 'admin' }, null],
			[{ userId: 1, name: 'Alice', role: 'user' }, null]
		];
		expect(view.materialize()).toStrictEqual(afterOrderRemoveExpected);
	});
});
