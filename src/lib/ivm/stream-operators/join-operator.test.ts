import { describe, expect, it } from 'vitest';
import { defaultComparator, Memory } from '../sources/memory.ts';
import { JoinOperator } from './join-operator.ts';
import { LogSink } from './log-operator.ts';
import { View } from '../sinks/view.ts';

describe('join-operator', () => {
	it('joins two sources on a common key', () => {
		// Setup: Create users source
		type User = { userId: number; name: string };
		type Order = { orderId: number; userId: number; amount: number };
		type JoinedRow = [User, Order];

		const users: User[] = [
			{ userId: 1, name: 'Alice' },
			{ userId: 2, name: 'Bob' }
		];

		const orders: Order[] = [
			{ orderId: 101, userId: 1, amount: 50 },
			{ orderId: 102, userId: 1, amount: 30 },
			{ orderId: 103, userId: 2, amount: 100 }
		];

		const usersSource = new Memory({ initialData: users, pk: 'userId', schema: null });
		const ordersSource = new Memory({ initialData: orders, pk: 'orderId', schema: null });

		// Comparator for joined results (sort by orderId)
		const resultComparator = (a: unknown, b: unknown): 0 | 1 | -1 => {
			const rowA = a as JoinedRow;
			const rowB = b as JoinedRow;
			const orderA = rowA[1];
			const orderB = rowB[1];
			if (orderA.orderId < orderB.orderId) return -1;
			if (orderA.orderId > orderB.orderId) return 1;
			return 0;
		};

		// Create join operator
		const usersConn = usersSource.connect();
		const ordersConn = ordersSource.connect();

		const joinOp = new JoinOperator<User, Order>(
			usersConn,
			ordersConn,
			(user) => user.userId,
			(order) => order.userId,
			resultComparator
		);

		const view = new View(joinOp, resultComparator);

		// Test 1: Initial join should produce 3 rows
		const initialExpected = [
			[
				{ userId: 1, name: 'Alice' },
				{ orderId: 101, userId: 1, amount: 50 }
			],
			[
				{ userId: 1, name: 'Alice' },
				{ orderId: 102, userId: 1, amount: 30 }
			],
			[
				{ userId: 2, name: 'Bob' },
				{ orderId: 103, userId: 2, amount: 100 }
			]
		];

		expect(view.materialize(), 'initial join results').toStrictEqual(initialExpected);

		// Test 2: Add new order for existing user (Alice)
		ordersSource.add({ orderId: 104, userId: 1, amount: 75 });

		const afterNewOrderExpected = [
			[
				{ userId: 1, name: 'Alice' },
				{ orderId: 101, userId: 1, amount: 50 }
			],
			[
				{ userId: 1, name: 'Alice' },
				{ orderId: 102, userId: 1, amount: 30 }
			],
			[
				{ userId: 2, name: 'Bob' },
				{ orderId: 103, userId: 2, amount: 100 }
			],
			[
				{ userId: 1, name: 'Alice' },
				{ orderId: 104, userId: 1, amount: 75 }
			]
		];

		expect(view.materialize(), 'after adding new order').toStrictEqual(afterNewOrderExpected);

		// Test 3: Add new user with no orders
		usersSource.add({ userId: 3, name: 'Charlie' });

		expect(view.materialize(), 'after adding user with no orders').toStrictEqual(
			afterNewOrderExpected
		);

		// Test 4: Add order for the new user
		ordersSource.add({ orderId: 105, userId: 3, amount: 200 });

		const finalExpected = [
			[
				{ userId: 1, name: 'Alice' },
				{ orderId: 101, userId: 1, amount: 50 }
			],
			[
				{ userId: 1, name: 'Alice' },
				{ orderId: 102, userId: 1, amount: 30 }
			],
			[
				{ userId: 2, name: 'Bob' },
				{ orderId: 103, userId: 2, amount: 100 }
			],
			[
				{ userId: 1, name: 'Alice' },
				{ orderId: 104, userId: 1, amount: 75 }
			],
			[
				{ userId: 3, name: 'Charlie' },
				{ orderId: 105, userId: 3, amount: 200 }
			]
		];

		expect(view.materialize(), 'after adding order for new user').toStrictEqual(finalExpected);
	});

	it('joins two sources on a common key with view as sink', () => {
		// Setup: Create users source
		type User = { userId: number; name: string };
		type Order = { orderId: number; userId: number; amount: number };
		type JoinedRow = [User, Order];

		const users: User[] = [
			{ userId: 1, name: 'Alice' },
			{ userId: 2, name: 'Bob' }
		];

		const orders: Order[] = [
			{ orderId: 101, userId: 1, amount: 50 },
			{ orderId: 102, userId: 1, amount: 30 },
			{ orderId: 103, userId: 2, amount: 100 }
		];

		const usersSource = new Memory({ initialData: users, pk: 'userId', schema: null });
		const ordersSource = new Memory({ initialData: orders, pk: 'orderId', schema: null });

		// Comparator for joined results (sort by orderId)
		const resultComparator = (a: unknown, b: unknown): 0 | 1 | -1 => {
			const rowA = a as JoinedRow;
			const rowB = b as JoinedRow;
			const orderA = rowA[1];
			const orderB = rowB[1];
			if (orderA.orderId < orderB.orderId) return -1;
			if (orderA.orderId > orderB.orderId) return 1;
			return 0;
		};

		// Create join operator
		const usersConn = usersSource.connect();
		const ordersConn = ordersSource.connect();

		const joinOp = new JoinOperator<User, Order>(
			usersConn,
			ordersConn,
			(user) => user.userId,
			(order) => order.userId,
			resultComparator
		);

		const view = new View(joinOp, resultComparator);

		// Test 1: Initial join should produce 3 rows
		const initialExpected = [
			[
				{ userId: 1, name: 'Alice' },
				{ orderId: 101, userId: 1, amount: 50 }
			],
			[
				{ userId: 1, name: 'Alice' },
				{ orderId: 102, userId: 1, amount: 30 }
			],
			[
				{ userId: 2, name: 'Bob' },
				{ orderId: 103, userId: 2, amount: 100 }
			]
		];

		expect(view.materialize(), 'initial join results').toStrictEqual(initialExpected);

		// Test 2: Add new order for existing user (Alice)
		ordersSource.add({ orderId: 104, userId: 1, amount: 75 });

		const afterNewOrderExpected = [
			[
				{ userId: 1, name: 'Alice' },
				{ orderId: 101, userId: 1, amount: 50 }
			],
			[
				{ userId: 1, name: 'Alice' },
				{ orderId: 102, userId: 1, amount: 30 }
			],
			[
				{ userId: 2, name: 'Bob' },
				{ orderId: 103, userId: 2, amount: 100 }
			],
			[
				{ userId: 1, name: 'Alice' },
				{ orderId: 104, userId: 1, amount: 75 }
			]
		];

		expect(view.materialize(), 'after adding new order').toStrictEqual(afterNewOrderExpected);

		// Test 3: Add new user with no orders
		usersSource.add({ userId: 3, name: 'Charlie' });

		expect(view.materialize(), 'after adding user with no orders').toStrictEqual(
			afterNewOrderExpected
		);

		// Test 4: Add order for the new user
		ordersSource.add({ orderId: 105, userId: 3, amount: 200 });

		const finalExpected = [
			[
				{ userId: 1, name: 'Alice' },
				{ orderId: 101, userId: 1, amount: 50 }
			],
			[
				{ userId: 1, name: 'Alice' },
				{ orderId: 102, userId: 1, amount: 30 }
			],
			[
				{ userId: 2, name: 'Bob' },
				{ orderId: 103, userId: 2, amount: 100 }
			],
			[
				{ userId: 1, name: 'Alice' },
				{ orderId: 104, userId: 1, amount: 75 }
			],
			[
				{ userId: 3, name: 'Charlie' },
				{ orderId: 105, userId: 3, amount: 200 }
			]
		];

		expect(view.materialize(), 'after adding order for new user').toStrictEqual(finalExpected);
	});
	it('joins two sources on a common key using subscribe', () => {
		// Setup: Create users source
		type User = { userId: number; name: string };
		type Order = { orderId: number; userId: number; amount: number };
		type JoinedRow = [User, Order];

		const users: User[] = [
			{ userId: 1, name: 'Alice' },
			{ userId: 2, name: 'Bob' }
		];

		const orders: Order[] = [
			{ orderId: 101, userId: 1, amount: 50 },
			{ orderId: 102, userId: 1, amount: 30 },
			{ orderId: 103, userId: 2, amount: 100 }
		];

		const usersSource = new Memory({ initialData: users, pk: 'userId', schema: null });
		const ordersSource = new Memory({ initialData: orders, pk: 'orderId', schema: null });

		// Comparator for joined results (sort by orderId)
		const resultComparator = (a: unknown, b: unknown): 0 | 1 | -1 => {
			const rowA = a as JoinedRow;
			const rowB = b as JoinedRow;
			const orderA = rowA[1];
			const orderB = rowB[1];
			if (orderA.orderId < orderB.orderId) return -1;
			if (orderA.orderId > orderB.orderId) return 1;
			return 0;
		};

		// Create join operator
		const usersConn = usersSource.connect();
		const ordersConn = ordersSource.connect();

		const joinOp = new JoinOperator<User, Order>(
			usersConn,
			ordersConn,
			(user) => user.userId,
			(order) => order.userId,
			resultComparator
		);

		const view = new View(joinOp, resultComparator);
		let results;
		view.subscribe((r) => {
			results = r;
		});

		// Test 1: Initial join should produce 3 rows
		const initialExpected = [
			[
				{ userId: 1, name: 'Alice' },
				{ orderId: 101, userId: 1, amount: 50 }
			],
			[
				{ userId: 1, name: 'Alice' },
				{ orderId: 102, userId: 1, amount: 30 }
			],
			[
				{ userId: 2, name: 'Bob' },
				{ orderId: 103, userId: 2, amount: 100 }
			]
		];

		expect(view.materialize(), 'initial join results').toStrictEqual(initialExpected);

		// Test 2: Add new order for existing user (Alice)
		ordersSource.add({ orderId: 104, userId: 1, amount: 75 });

		const afterNewOrderExpected = [
			[
				{ userId: 1, name: 'Alice' },
				{ orderId: 101, userId: 1, amount: 50 }
			],
			[
				{ userId: 1, name: 'Alice' },
				{ orderId: 102, userId: 1, amount: 30 }
			],
			[
				{ userId: 2, name: 'Bob' },
				{ orderId: 103, userId: 2, amount: 100 }
			],
			[
				{ userId: 1, name: 'Alice' },
				{ orderId: 104, userId: 1, amount: 75 }
			]
		];

		expect(results, 'after adding new order').toStrictEqual(afterNewOrderExpected);

		// Test 3: Add new user with no orders
		usersSource.add({ userId: 3, name: 'Charlie' });

		expect(results, 'after adding user with no orders').toStrictEqual(afterNewOrderExpected);

		// Test 4: Add order for the new user
		ordersSource.add({ orderId: 105, userId: 3, amount: 200 });

		const finalExpected = [
			[
				{ userId: 1, name: 'Alice' },
				{ orderId: 101, userId: 1, amount: 50 }
			],
			[
				{ userId: 1, name: 'Alice' },
				{ orderId: 102, userId: 1, amount: 30 }
			],
			[
				{ userId: 2, name: 'Bob' },
				{ orderId: 103, userId: 2, amount: 100 }
			],
			[
				{ userId: 1, name: 'Alice' },
				{ orderId: 104, userId: 1, amount: 75 }
			],
			[
				{ userId: 3, name: 'Charlie' },
				{ orderId: 105, userId: 3, amount: 200 }
			]
		];

		expect(results, 'after adding order for new user').toStrictEqual(finalExpected);
	});
	it('removes joined rows when source rows are deleted', () => {
		type User = { userId: number; name: string };
		type Order = { orderId: number; userId: number; amount: number };
		type JoinedRow = [User, Order];

		const users: User[] = [
			{ userId: 1, name: 'Alice' },
			{ userId: 2, name: 'Bob' }
		];

		const orders: Order[] = [
			{ orderId: 101, userId: 1, amount: 50 },
			{ orderId: 102, userId: 1, amount: 30 },
			{ orderId: 103, userId: 2, amount: 100 }
		];

		const usersSource = new Memory({ initialData: users, pk: 'userId', schema: null });
		const ordersSource = new Memory({ initialData: orders, pk: 'orderId', schema: null });

		const resultComparator = (a: unknown, b: unknown): 0 | 1 | -1 => {
			const rowA = a as JoinedRow;
			const rowB = b as JoinedRow;
			const orderA = rowA[1];
			const orderB = rowB[1];
			if (orderA.orderId < orderB.orderId) return -1;
			if (orderA.orderId > orderB.orderId) return 1;
			return 0;
		};

		const usersConn = usersSource.connect();
		const ordersConn = ordersSource.connect();

		const joinOp = new JoinOperator<User, Order>(
			usersConn,
			ordersConn,
			(user) => user.userId,
			(order) => order.userId,
			resultComparator
		);

		const view = new View(joinOp, resultComparator);
		// Initial state: 3 joined rows
		const initialExpected = [
			[
				{ userId: 1, name: 'Alice' },
				{ orderId: 101, userId: 1, amount: 50 }
			],
			[
				{ userId: 1, name: 'Alice' },
				{ orderId: 102, userId: 1, amount: 30 }
			],
			[
				{ userId: 2, name: 'Bob' },
				{ orderId: 103, userId: 2, amount: 100 }
			]
		];
		expect(view.materialize(), 'initial join results').toStrictEqual(initialExpected);

		// Test 1: Delete one order (orderId: 102) from right side
		ordersSource.remove({ orderId: 102, userId: 1, amount: 30 });

		const afterOrderDeleteExpected = [
			[
				{ userId: 1, name: 'Alice' },
				{ orderId: 101, userId: 1, amount: 50 }
			],
			[
				{ userId: 2, name: 'Bob' },
				{ orderId: 103, userId: 2, amount: 100 }
			]
		];
		expect(view.materialize(), 'after deleting order 102').toStrictEqual(afterOrderDeleteExpected);

		// Test 2: Delete user (Alice) from left side - should remove all her orders
		usersSource.remove({ userId: 1, name: 'Alice' });

		const afterUserDeleteExpected = [
			[
				{ userId: 2, name: 'Bob' },
				{ orderId: 103, userId: 2, amount: 100 }
			]
		];
		expect(view.materialize(), 'after deleting user Alice').toStrictEqual(afterUserDeleteExpected);

		// Test 3: Delete the last order
		ordersSource.remove({ orderId: 103, userId: 2, amount: 100 });
		usersSource.remove({ userId: 2, name: 'Bob' });

		expect(view.materialize(), 'after deleting all joined rows').toStrictEqual([]);

		// Test 4: Verify user still exists (no matching orders)
		usersSource.add({ userId: 2, name: 'Bob' }); // Re-add Bob
		expect(view.materialize(), 'user with no matching orders').toStrictEqual([]);
	});
	it('handles updates as delete+add pairs', () => {
		type User = { userId: number; name: string };
		type Order = { orderId: number; userId: number; amount: number };
		type JoinedRow = [User, Order];

		const users: User[] = [
			{ userId: 1, name: 'Alice' },
			{ userId: 2, name: 'Bob' }
		];

		const orders: Order[] = [
			{ orderId: 101, userId: 1, amount: 50 },
			{ orderId: 102, userId: 2, amount: 100 }
		];

		const usersSource = new Memory({ initialData: users, pk: 'userId', schema: null });
		const ordersSource = new Memory({ initialData: orders, pk: 'orderId', schema: null });

		const resultComparator = (a: unknown, b: unknown): 0 | 1 | -1 => {
			const rowA = a as JoinedRow;
			const rowB = b as JoinedRow;
			const orderA = rowA[1];
			const orderB = rowB[1];
			if (orderA.orderId < orderB.orderId) return -1;
			if (orderA.orderId > orderB.orderId) return 1;
			return 0;
		};

		const usersConn = usersSource.connect();
		const ordersConn = ordersSource.connect();

		const joinOp = new JoinOperator<User, Order>(
			usersConn,
			ordersConn,
			(user) => user.userId,
			(order) => order.userId,
			resultComparator
		);

		const view = new View(joinOp, resultComparator);

		// Initial state
		const initialExpected = [
			[
				{ userId: 1, name: 'Alice' },
				{ orderId: 101, userId: 1, amount: 50 }
			],
			[
				{ userId: 2, name: 'Bob' },
				{ orderId: 102, userId: 2, amount: 100 }
			]
		];
		expect(view.materialize(), 'initial state').toStrictEqual(initialExpected);

		// Test 1: Update order amount (delete old, add new)
		ordersSource.remove({ orderId: 101, userId: 1, amount: 50 });
		ordersSource.add({ orderId: 101, userId: 1, amount: 75 }); // Updated amount

		const afterAmountUpdateExpected = [
			[
				{ userId: 1, name: 'Alice' },
				{ orderId: 101, userId: 1, amount: 75 }
			],
			[
				{ userId: 2, name: 'Bob' },
				{ orderId: 102, userId: 2, amount: 100 }
			]
		];
		expect(view.materialize(), 'after updating order amount').toStrictEqual(
			afterAmountUpdateExpected
		);

		// Test 2: Update order to change join key (reassign order to different user)
		ordersSource.remove({ orderId: 101, userId: 1, amount: 75 });
		ordersSource.add({ orderId: 101, userId: 2, amount: 75 }); // Now belongs to Bob

		const afterKeyChangeExpected = [
			[
				{ userId: 2, name: 'Bob' },
				{ orderId: 101, userId: 2, amount: 75 }
			],
			[
				{ userId: 2, name: 'Bob' },
				{ orderId: 102, userId: 2, amount: 100 }
			]
		];
		expect(view.materialize(), 'after changing join key').toStrictEqual(afterKeyChangeExpected);

		// Test 3: Update user name (delete old, add new)
		usersSource.remove({ userId: 2, name: 'Bob' });
		usersSource.add({ userId: 2, name: 'Robert' }); // Updated name

		const afterNameUpdateExpected = [
			[
				{ userId: 2, name: 'Robert' },
				{ orderId: 101, userId: 2, amount: 75 }
			],
			[
				{ userId: 2, name: 'Robert' },
				{ orderId: 102, userId: 2, amount: 100 }
			]
		];
		expect(view.materialize(), 'after updating user name').toStrictEqual(afterNameUpdateExpected);

		// Test 4: Update user ID (changes join key on left side)
		usersSource.remove({ userId: 2, name: 'Robert' });
		usersSource.add({ userId: 3, name: 'Robert' }); // Changed userId

		// Orders with userId: 2 no longer have matching user
		expect(view.materialize(), 'after changing user join key').toStrictEqual([]);

		// Add matching order for new userId
		ordersSource.add({ orderId: 103, userId: 3, amount: 200 });

		const finalExpected = [
			[
				{ userId: 3, name: 'Robert' },
				{ orderId: 103, userId: 3, amount: 200 }
			]
		];
		expect(view.materialize(), 'after adding order for updated user').toStrictEqual(finalExpected);
	});
});
