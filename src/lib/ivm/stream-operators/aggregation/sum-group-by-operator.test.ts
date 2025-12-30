import { describe, expect, it } from 'vitest';
import { Memory } from '../../sources/memory.ts';
import { SumGroupByOperator } from './sum-group-by-operator.ts';
import { GroupByOperator } from '../group-by-operator.ts';
import { View } from '../../sinks/view.ts';

describe('SumGroupByOperator', () => {
	it('sums rows per group (SUM with GROUP BY)', () => {
		type Row = { userId: number; id: number; amount: number };
		const initialData: Row[] = [
			{ userId: 1, id: 1, amount: 10 },
			{ userId: 1, id: 2, amount: 20 },
			{ userId: 2, id: 3, amount: 30 },
			{ userId: 3, id: 4, amount: 40 }
		];

		const source = new Memory({ initialData, pk: 'id', schema: null });
		const conn = source.connect() as any;

		const rowComparator = (a: Row, b: Row) => {
			if (a.id < b.id) return -1;
			if (a.id > b.id) return 1;
			return 0;
		};

		const groupBy = new GroupByOperator(conn, ['userId'], rowComparator);
		const sum = new SumGroupByOperator(groupBy, 'amount');
		const view = new View(sum, (a, b) => a.userId - b.userId);

		// Initial sums per group
		expect(view.materialize(), 'initial sum is correct').toStrictEqual([
			{ userId: 1, sum: 30 },
			{ userId: 2, sum: 30 },
			{ userId: 3, sum: 40 }
		]);

		// Add row to existing group
		source.add({ userId: 1, id: 5, amount: 50 });
		expect(view.materialize(), 'after adding to userId 1').toStrictEqual([
			{ userId: 1, sum: 80 },
			{ userId: 2, sum: 30 },
			{ userId: 3, sum: 40 }
		]);

		// Add row to new group
		source.add({ userId: 4, id: 6, amount: 60 });
		expect(view.materialize(), 'after adding to userId 4').toStrictEqual([
			{ userId: 1, sum: 80 },
			{ userId: 2, sum: 30 },
			{ userId: 3, sum: 40 },
			{ userId: 4, sum: 60 }
		]);

		// Remove row from group
		source.remove({ userId: 1, id: 1, amount: 10 });
		expect(view.materialize(), 'after removing id 1').toStrictEqual([
			{ userId: 1, sum: 70 },
			{ userId: 2, sum: 30 },
			{ userId: 3, sum: 40 },
			{ userId: 4, sum: 60 }
		]);
		// Remove last row from group (group should disappear)
		source.remove({ userId: 3, id: 4, amount: 40 });
		expect(view.materialize()).toStrictEqual([
			{ userId: 1, sum: 70 },
			{ userId: 2, sum: 30 },
			{ userId: 4, sum: 60 }
		]);
	});

	it('sums rows per group (SUM with GROUP BY) when subscribed to View', () => {
		type Row = { userId: number; id: number; amount: number };
		const initialData: Row[] = [
			{ userId: 1, id: 1, amount: 10 },
			{ userId: 1, id: 2, amount: 20 },
			{ userId: 2, id: 3, amount: 30 },
			{ userId: 3, id: 4, amount: 40 }
		];

		const source = new Memory({ initialData, pk: 'id', schema: null });
		const conn = source.connect();

		const rowComparator = (a: Row, b: Row) => {
			if (a.id < b.id) return -1;
			if (a.id > b.id) return 1;
			return 0;
		};

		const groupBy = new GroupByOperator(conn, ['userId'], rowComparator);
		const sum = new SumGroupByOperator(groupBy, 'amount');
		const view = new View(sum, (a, b) => a.userId - b.userId);

		// Initial sums per group
		expect(view.materialize(), 'initial sum is correct').toStrictEqual([
			{ userId: 1, sum: 30 },
			{ userId: 2, sum: 30 },
			{ userId: 3, sum: 40 }
		]);
		let results;

		view.subscribe((res) => {
			results = res;
		});

		expect(results).toStrictEqual([
			{ userId: 1, sum: 30 },
			{ userId: 2, sum: 30 },
			{ userId: 3, sum: 40 }
		]);

		// Add row to existing group
		source.add({ userId: 1, id: 5, amount: 50 });
		expect(results, 'after adding to userId 1').toStrictEqual([
			{ userId: 1, sum: 80 },
			{ userId: 2, sum: 30 },
			{ userId: 3, sum: 40 }
		]);

		// Add row to new group
		source.add({ userId: 4, id: 6, amount: 60 });
		expect(results, 'after adding to userId 4').toStrictEqual([
			{ userId: 1, sum: 80 },
			{ userId: 2, sum: 30 },
			{ userId: 3, sum: 40 },
			{ userId: 4, sum: 60 }
		]);

		// Remove row from group
		source.remove({ userId: 1, id: 1, amount: 10 });
		expect(results, 'after removing id 1').toStrictEqual([
			{ userId: 1, sum: 70 },
			{ userId: 2, sum: 30 },
			{ userId: 3, sum: 40 },
			{ userId: 4, sum: 60 }
		]);

		// // Remove last row from group (group should disappear)
		source.remove({ userId: 3, id: 4, amount: 40 });
		expect(results).toStrictEqual([
			{ userId: 1, sum: 70 },
			{ userId: 2, sum: 30 },
			{ userId: 4, sum: 60 }
		]);
	});

	it('handles row updates changing the grouping key', () => {
		type Row = { userId: number; id: number; amount: number };
		const initialData: Row[] = [
			{ userId: 1, id: 1, amount: 10 },
			{ userId: 2, id: 2, amount: 20 }
		];

		const source = new Memory({ initialData, pk: 'id', schema: null });
		const conn = source.connect() as any;
		const rowComparator = (a: Row, b: Row) => a.id - b.id;

		const groupBy = new GroupByOperator(conn, ['userId'], rowComparator);
		const sum = new SumGroupByOperator(groupBy, 'amount');
		const view = new View(sum, (a, b) => a.userId - b.userId);

		expect(view.materialize()).toStrictEqual([
			{ userId: 1, sum: 10 },
			{ userId: 2, sum: 20 }
		]);

		// Move id:1 from userId:1 to userId:2
		source.update({ userId: 1, id: 1, amount: 10 }, { userId: 2 });

		expect(view.materialize()).toStrictEqual([
			{ userId: 2, sum: 30 }
			// userId:1 should be gone because sum dropped to 0
		]);
	});

	it('handles row updates NOT changing the grouping key', () => {
		type Row = { userId: number; id: number; amount: number };
		const initialData: Row[] = [{ userId: 1, id: 1, amount: 10 }];

		const source = new Memory({ initialData, pk: 'id', schema: null });
		const conn = source.connect() as any;
		const rowComparator = (a: Row, b: Row) => a.id - b.id;

		const groupBy = new GroupByOperator(conn, ['userId'], rowComparator);
		const sum = new SumGroupByOperator(groupBy, 'amount');
		const view = new View(sum, (a, b) => a.userId - b.userId);

		expect(view.materialize()).toStrictEqual([{ userId: 1, sum: 10 }]);

		// Update amount only
		source.update({ userId: 1, id: 1, amount: 10 }, { amount: 50 });

		expect(view.materialize()).toStrictEqual([{ userId: 1, sum: 50 }]);
	});

	it('supports grouping by multiple keys', () => {
		type Row = { region: string; type: string; id: number; amount: number };
		const initialData: Row[] = [
			{ region: 'US', type: 'A', id: 1, amount: 30 },
			{ region: 'US', type: 'A', id: 2, amount: 20 },
			{ region: 'US', type: 'B', id: 3, amount: 10 },
			{ region: 'EU', type: 'A', id: 4, amount: 1 }
		];

		const source = new Memory({ initialData, pk: 'id', schema: null });
		const conn = source.connect() as any;
		const rowComparator = (a: Row, b: Row) => a.id - b.id;

		const groupBy = new GroupByOperator(conn, ['region', 'type'], rowComparator);
		const sum = new SumGroupByOperator(groupBy, 'amount');
		const view = new View(sum, (a, b) => {
			for (const key of ['region', 'type']) {
				const valA = a[key];
				const valB = b[key];
				if (valA === valB) continue;
				if (valA === undefined || valA === null) return -1;
				if (valB === undefined || valB === null) return 1;
				if (valA < valB) return -1;
				if (valA > valB) return 1;
			}
			return 0;
		});

		expect(view.materialize()).toStrictEqual([
			{ region: 'EU', type: 'A', sum: 1 },
			{ region: 'US', type: 'A', sum: 50 },
			{ region: 'US', type: 'B', sum: 10 }
		]);

		// Add another US/B
		source.add({ region: 'US', type: 'B', id: 5, amount: 80 });

		expect(view.materialize()).toStrictEqual([
			{ region: 'EU', type: 'A', sum: 1 },
			{ region: 'US', type: 'A', sum: 50 },
			{ region: 'US', type: 'B', sum: 90 }
		]);
	});

	it('correctly initializes from pre-populated source', () => {
		type Row = { userId: number; id: number; amount: number };
		const initialData: Row[] = [
			{ userId: 1, id: 1, amount: 1 },
			{ userId: 1, id: 2, amount: 10 },
			{ userId: 2, id: 3, amount: 100 }
		];
		// Setup source but don't connect yet
		const source = new Memory({ initialData, pk: 'id', schema: null });

		// Add more data before connection
		source.add({ userId: 3, id: 4, amount: 1000 });

		const conn = source.connect();
		const rowComparator = (a: Row, b: Row) => a.id - b.id;
		const groupBy = new GroupByOperator(conn, ['userId'], rowComparator);
		const sum = new SumGroupByOperator(groupBy, 'amount');
		const view = new View(sum, (a, b) => a.userId - b.userId);

		// Should reflect all data immediately
		expect(view.materialize()).toStrictEqual([
			{ userId: 1, sum: 11 },
			{ userId: 2, sum: 100 },
			{ userId: 3, sum: 1000 }
		]);
	});

	it('handles oscillation (add/remove/add)', () => {
		type Row = { userId: number; id: number; amount: 10 };
		const source = new Memory<Row>({ initialData: [], pk: 'id', schema: null });
		const conn = source.connect() as any;
		const rowComparator = (a: Row, b: Row) => a.id - b.id;

		const groupBy = new GroupByOperator(conn, ['userId'], rowComparator);
		const sum = new SumGroupByOperator(groupBy, 'amount');
		const view = new View(sum, (a, b) => a.userId - b.userId);

		expect(view.materialize()).toStrictEqual([]);
		// Add
		source.add({ userId: 1, id: 1, amount: 10 });
		expect(view.materialize()).toStrictEqual([{ userId: 1, sum: 10 }]);

		// Remove (group disappears)
		source.remove({ userId: 1, id: 1, amount: 10 });
		expect(view.materialize()).toStrictEqual([]);

		// Add again (group reappears)
		source.add({ userId: 1, id: 1, amount: 1 });
		expect(view.materialize()).toStrictEqual([{ userId: 1, sum: 1 }]);
	});
	it('handles negatives and zeroes', () => {
		type Row = { userId: number; id: number; amount: number };
		const initialData: Row[] = [
			{ userId: 1, id: 1, amount: -10 },
			{ userId: 1, id: 2, amount: -100 },
			{ userId: 2, id: 3, amount: 100 }
		];

		const source = new Memory({ initialData, pk: 'id', schema: null });

		// Add more data before connection
		source.add({ userId: 2, id: 4, amount: 1000 });

		const conn = source.connect();
		const rowComparator = (a: Row, b: Row) => a.id - b.id;
		const groupBy = new GroupByOperator(conn, ['userId'], rowComparator);
		const sum = new SumGroupByOperator(groupBy, 'amount'); // ✅ Changed from MaxGroupByOperator
		const view = new View(sum, (a, b) => a.userId - b.userId);

		// Initial: userId 1 has -110, userId 2 has 1100
		expect(view.materialize(), 'initial sum with negatives').toStrictEqual([
			{ userId: 1, sum: -110 },
			{ userId: 2, sum: 1100 }
		]);

		// Add another negative to userId 1
		source.add({ userId: 1, id: 5, amount: -5 });
		expect(view.materialize(), 'after adding another negative').toStrictEqual([
			{ userId: 1, sum: -115 },
			{ userId: 2, sum: 1100 }
		]);

		// Add zero to userId 1 (should not change sum)
		source.add({ userId: 1, id: 6, amount: 0 });
		expect(view.materialize(), 'after adding zero').toStrictEqual([
			{ userId: 1, sum: -115 },
			{ userId: 2, sum: 1100 }
		]);

		// Add positive to bring userId 1 sum closer to zero
		source.add({ userId: 1, id: 7, amount: 115 });
		expect(view.materialize(), 'sum reaches exactly zero').toStrictEqual([
			{ userId: 1, sum: 0 }, // Group should still exist with sum of 0
			{ userId: 2, sum: 1100 }
		]);

		// Remove a value to make sum negative again
		source.remove({ userId: 1, id: 7, amount: 115 });
		expect(view.materialize(), 'after removing positive value').toStrictEqual([
			{ userId: 1, sum: -115 },
			{ userId: 2, sum: 1100 }
		]);

		// Create a new group starting with zero
		source.add({ userId: 3, id: 8, amount: 0 });
		expect(view.materialize(), 'new group with zero value').toStrictEqual([
			{ userId: 1, sum: -115 },
			{ userId: 2, sum: 1100 },
			{ userId: 3, sum: 0 }
		]);
	});
});
