import { describe, expect, it } from 'vitest';
import { Memory } from '../sources/memory.ts';
import { View } from '../sinks/view.ts';
import { DistinctOnOperator } from './distinct-on-operator.ts';

describe('distinct on ', () => {
	it('keeps one row per unique key (first occurrence)', () => {
		type Order = { orderId: number; userId: number; amount: number; timestamp: number };

		const orders: Order[] = [
			{ orderId: 1, userId: 100, amount: 50, timestamp: 1000 },
			{ orderId: 2, userId: 100, amount: 75, timestamp: 2000 }, // Same userId
			{ orderId: 3, userId: 200, amount: 100, timestamp: 1500 },
			{ orderId: 4, userId: 100, amount: 25, timestamp: 3000 }, // Same userId again
			{ orderId: 5, userId: 200, amount: 150, timestamp: 2500 } // Same userId
		];

		const source = new Memory({ initialData: orders, pk: 'orderId', schema: {} });
		const conn = source.connect();

		// DISTINCT ON (userId) - no row comparator, so keeps first row per userId
		const distinctOn = new DistinctOnOperator(
			conn,
			(order) => order.userId,
			(a, b) => a - b
			// No rowComparator - keeps first row
		);

		const comparator = (a: Order, b: Order) => a.userId - b.userId;
		const view = new View(distinctOn, comparator);
		const results = view.materialize();

		// Should keep first order for each userId
		expect(results).toStrictEqual([
			{ orderId: 1, userId: 100, amount: 50, timestamp: 1000 }, // First for userId 100
			{ orderId: 3, userId: 200, amount: 100, timestamp: 1500 } // First for userId 200
		]);
	});
	it('keeps the "best" row per key based on row comparator', () => {
		type Order = { orderId: number; userId: number; amount: number; timestamp: number };

		const orders: Order[] = [
			{ orderId: 1, userId: 100, amount: 50, timestamp: 1000 },
			{ orderId: 2, userId: 100, amount: 75, timestamp: 2000 }, // Highest amount for 100
			{ orderId: 3, userId: 200, amount: 100, timestamp: 1500 },
			{ orderId: 4, userId: 100, amount: 25, timestamp: 3000 },
			{ orderId: 5, userId: 200, amount: 150, timestamp: 2500 } // Highest amount for 200
		];

		const source = new Memory({ initialData: orders, pk: 'orderId', schema: {} });
		const conn = source.connect();

		// DISTINCT ON (userId) keeping highest amount
		const distinctOn = new DistinctOnOperator(
			conn,
			(order) => order.userId,
			(a, b) => a - b,
			(a, b) => b.amount - a.amount // Keep row with highest amount (desc)
		);

		const comparator = (a: Order, b: Order) => a.userId - b.userId;
		const view = new View(distinctOn, comparator);
		const results = view.materialize();

		// Should keep highest-amount order for each userId
		expect(results).toStrictEqual([
			{ orderId: 2, userId: 100, amount: 75, timestamp: 2000 }, // Highest for 100
			{ orderId: 5, userId: 200, amount: 150, timestamp: 2500 } // Highest for 200
		]);
	});
	it('replaces row when better match arrives (incremental)', () => {
		type Order = { orderId: number; userId: number; amount: number; timestamp: number };

		const initialOrders: Order[] = [
			{ orderId: 1, userId: 100, amount: 50, timestamp: 1000 },
			{ orderId: 2, userId: 200, amount: 100, timestamp: 1500 }
		];

		const source = new Memory({ initialData: initialOrders, pk: 'orderId', schema: {} });
		const conn = source.connect();

		// DISTINCT ON (userId) keeping most recent (highest timestamp)
		const distinctOn = new DistinctOnOperator(
			conn,
			(order) => order.userId,
			(a, b) => a - b,
			(a, b) => b.timestamp - a.timestamp // Keep most recent
		);

		const comparator = (a: Order, b: Order) => a.userId - b.userId;
		const view = new View(distinctOn, comparator);

		let results = view.materialize();
		view.subscribe((r) => {
			results = r;
		});
		// Initial state
		expect(results).toStrictEqual([
			{ orderId: 1, userId: 100, amount: 50, timestamp: 1000 },
			{ orderId: 2, userId: 200, amount: 100, timestamp: 1500 }
		]);

		// Add a more recent order for userId 100
		source.add({ orderId: 3, userId: 100, amount: 75, timestamp: 2000 });

		// Should replace orderId:1 with orderId:3 (more recent)
		expect(results).toStrictEqual([
			{ orderId: 3, userId: 100, amount: 75, timestamp: 2000 }, // Replaced!
			{ orderId: 2, userId: 200, amount: 100, timestamp: 1500 }
		]);

		// Add an older order for userId 100 (should NOT replace)
		source.add({ orderId: 4, userId: 100, amount: 90, timestamp: 500 });

		// Should still have orderId:3 (it's more recent)
		expect(results).toStrictEqual([
			{ orderId: 3, userId: 100, amount: 75, timestamp: 2000 }, // Still here
			{ orderId: 2, userId: 200, amount: 100, timestamp: 1500 }
		]);

		// Remove the current "best" row (orderId:3 with timestamp 2000)
		source.remove({ orderId: 3, userId: 100, amount: 75, timestamp: 2000 });

		// Should fall back to next best available (orderId:1 with timestamp 1000)
		// orderId:4 has timestamp 500, so orderId:1 is more recent
		expect(results).toStrictEqual([
			{ orderId: 1, userId: 100, amount: 50, timestamp: 1000 }, // Fallback!
			{ orderId: 2, userId: 200, amount: 100, timestamp: 1500 }
		]);
	});
	it('maintains reference counts when same key added multiple times', () => {
		type Order = { orderId: number; userId: number; amount: number; timestamp: number };

		const initialOrders: Order[] = [
			{ orderId: 1, userId: 100, amount: 50, timestamp: 1000 },
			{ orderId: 2, userId: 100, amount: 75, timestamp: 2000 }, // Better (higher amount)
			{ orderId: 3, userId: 100, amount: 60, timestamp: 1500 } // Middle
		];

		const source = new Memory({ initialData: initialOrders, pk: 'orderId', schema: {} });
		const conn = source.connect();

		// DISTINCT ON (userId) keeping highest amount
		const distinctOn = new DistinctOnOperator(
			conn,
			(order) => order.userId,
			(a, b) => a - b,
			(a, b) => b.amount - a.amount // Keep highest amount
		);

		const comparator = (a: Order, b: Order) => a.userId - b.userId;
		const view = new View(distinctOn, comparator);

		// Initial: Should show orderId:2 (highest amount: 75)
		expect(view.materialize()).toStrictEqual([
			{ orderId: 2, userId: 100, amount: 75, timestamp: 2000 }
		]);

		// Add another order for userId 100 with lower amount
		source.add({ orderId: 4, userId: 100, amount: 40, timestamp: 3000 });

		let results = view.materialize();
		view.subscribe((r) => {
			results = r;
		});
		// Should still show orderId:2 (still the highest)
		// But internally, count should be 4 (four rows with userId: 100)
		expect(results).toStrictEqual([{ orderId: 2, userId: 100, amount: 75, timestamp: 2000 }]);

		// Remove orderId:1 (count goes from 4 to 3)
		source.remove({ orderId: 1, userId: 100, amount: 50, timestamp: 1000 });

		// Should still show orderId:2 (still the best among remaining)
		expect(results).toStrictEqual([{ orderId: 2, userId: 100, amount: 75, timestamp: 2000 }]);

		// Remove orderId:3 (count goes from 3 to 2)
		source.remove({ orderId: 3, userId: 100, amount: 60, timestamp: 1500 });

		// Should still show orderId:2
		expect(results).toStrictEqual([{ orderId: 2, userId: 100, amount: 75, timestamp: 2000 }]);

		// Remove orderId:2 (the current "best" row, count goes from 2 to 1)
		source.remove({ orderId: 2, userId: 100, amount: 75, timestamp: 2000 });

		// Now should show orderId:4 (the only remaining row for userId 100)
		expect(results).toStrictEqual([{ orderId: 4, userId: 100, amount: 40, timestamp: 3000 }]);

		// Remove orderId:4 (count goes from 1 to 0)
		source.remove({ orderId: 4, userId: 100, amount: 40, timestamp: 3000 });

		// Should be empty (no more rows for userId 100)
		expect(results).toStrictEqual([]);
	});
});
