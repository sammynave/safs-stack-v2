import { describe, expect, it } from 'vitest';
import { Memory } from '../../sources/memory.ts';
import { AvgGroupByOperator } from './avg-group-by-operator.ts';
import { GroupByOperator } from '../group-by-operator.ts';
import { View } from '../../sinks/view.ts';

describe('AvgGroupByOperator', () => {
	it('averages rows per group (AVG with GROUP BY)', () => {
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
		const avg = new AvgGroupByOperator(groupBy, 'amount');
		const view = new View(avg, (a, b) => a.userId - b.userId);

		// Initial avgs per group
		expect(view.materialize(), 'initial avg is correct').toStrictEqual([
			{ userId: 1, avg: 15 }, // (10 + 20) / 2 = 15
			{ userId: 2, avg: 30 }, // 30 / 1 = 30
			{ userId: 3, avg: 40 } // 40 / 1 = 40
		]);

		// Add row to existing group
		source.add({ userId: 1, id: 5, amount: 30 });
		expect(view.materialize(), 'after adding to userId 1').toStrictEqual([
			{ userId: 1, avg: 20 }, // (10 + 20 + 30) / 3 = 20
			{ userId: 2, avg: 30 },
			{ userId: 3, avg: 40 }
		]);

		// Add row to new group
		source.add({ userId: 4, id: 6, amount: 60 });
		expect(view.materialize(), 'after adding to userId 4').toStrictEqual([
			{ userId: 1, avg: 20 },
			{ userId: 2, avg: 30 },
			{ userId: 3, avg: 40 },
			{ userId: 4, avg: 60 }
		]);

		// Remove row from group
		source.remove({ userId: 1, id: 1, amount: 10 });
		expect(view.materialize(), 'after removing id 1').toStrictEqual([
			{ userId: 1, avg: 25 }, // (20 + 30) / 2 = 25
			{ userId: 2, avg: 30 },
			{ userId: 3, avg: 40 },
			{ userId: 4, avg: 60 }
		]);

		// Remove last row from group (group should disappear)
		source.remove({ userId: 3, id: 4, amount: 40 });
		expect(view.materialize()).toStrictEqual([
			{ userId: 1, avg: 25 },
			{ userId: 2, avg: 30 },
			{ userId: 4, avg: 60 }
		]);
	});

	it('averages rows per group (AVG with GROUP BY) when subscribed to View', () => {
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
		const avg = new AvgGroupByOperator(groupBy, 'amount');
		const view = new View(avg, (a, b) => a.userId - b.userId);

		// Initial avgs per group
		expect(view.materialize(), 'initial avg is correct').toStrictEqual([
			{ userId: 1, avg: 15 },
			{ userId: 2, avg: 30 },
			{ userId: 3, avg: 40 }
		]);
		let results;

		view.subscribe((res) => {
			results = res;
		});

		expect(results).toStrictEqual([
			{ userId: 1, avg: 15 },
			{ userId: 2, avg: 30 },
			{ userId: 3, avg: 40 }
		]);

		// Add row to existing group
		source.add({ userId: 1, id: 5, amount: 30 });
		expect(results, 'after adding to userId 1').toStrictEqual([
			{ userId: 1, avg: 20 },
			{ userId: 2, avg: 30 },
			{ userId: 3, avg: 40 }
		]);

		// Add row to new group
		source.add({ userId: 4, id: 6, amount: 60 });
		expect(results, 'after adding to userId 4').toStrictEqual([
			{ userId: 1, avg: 20 },
			{ userId: 2, avg: 30 },
			{ userId: 3, avg: 40 },
			{ userId: 4, avg: 60 }
		]);

		// Remove row from group
		source.remove({ userId: 1, id: 1, amount: 10 });
		expect(results, 'after removing id 1').toStrictEqual([
			{ userId: 1, avg: 25 },
			{ userId: 2, avg: 30 },
			{ userId: 3, avg: 40 },
			{ userId: 4, avg: 60 }
		]);

		// Remove last row from group (group should disappear)
		source.remove({ userId: 3, id: 4, amount: 40 });
		expect(results).toStrictEqual([
			{ userId: 1, avg: 25 },
			{ userId: 2, avg: 30 },
			{ userId: 4, avg: 60 }
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
		const avg = new AvgGroupByOperator(groupBy, 'amount');
		const view = new View(avg, (a, b) => a.userId - b.userId);

		expect(view.materialize()).toStrictEqual([
			{ userId: 1, avg: 10 },
			{ userId: 2, avg: 20 }
		]);

		// Move id:1 from userId:1 to userId:2
		source.update({ userId: 1, id: 1, amount: 10 }, { userId: 2 });

		expect(view.materialize()).toStrictEqual([
			{ userId: 2, avg: 15 } // (10 + 20) / 2 = 15
			// userId:1 should be gone because group is empty
		]);
	});

	it('handles row updates NOT changing the grouping key', () => {
		type Row = { userId: number; id: number; amount: number };
		const initialData: Row[] = [
			{ userId: 1, id: 1, amount: 10 },
			{ userId: 1, id: 2, amount: 20 }
		];

		const source = new Memory({ initialData, pk: 'id', schema: null });
		const conn = source.connect() as any;
		const rowComparator = (a: Row, b: Row) => a.id - b.id;

		const groupBy = new GroupByOperator(conn, ['userId'], rowComparator);
		const avg = new AvgGroupByOperator(groupBy, 'amount');
		const view = new View(avg, (a, b) => a.userId - b.userId);

		expect(view.materialize()).toStrictEqual([{ userId: 1, avg: 15 }]);

		// Update amount only
		source.update({ userId: 1, id: 1, amount: 10 }, { amount: 50 });

		expect(view.materialize()).toStrictEqual([{ userId: 1, avg: 35 }]); // (50 + 20) / 2 = 35
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
		const avg = new AvgGroupByOperator(groupBy, 'amount');
		const view = new View(avg, (a, b) => {
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
			{ region: 'EU', type: 'A', avg: 1 },
			{ region: 'US', type: 'A', avg: 25 }, // (30 + 20) / 2 = 25
			{ region: 'US', type: 'B', avg: 10 }
		]);

		// Add another US/B
		source.add({ region: 'US', type: 'B', id: 5, amount: 50 });

		expect(view.materialize()).toStrictEqual([
			{ region: 'EU', type: 'A', avg: 1 },
			{ region: 'US', type: 'A', avg: 25 },
			{ region: 'US', type: 'B', avg: 30 } // (10 + 50) / 2 = 30
		]);
	});

	it('correctly initializes from pre-populated source', () => {
		type Row = { userId: number; id: number; amount: number };
		const initialData: Row[] = [
			{ userId: 1, id: 1, amount: 10 },
			{ userId: 1, id: 2, amount: 20 },
			{ userId: 2, id: 3, amount: 100 }
		];
		// Setup source but don't connect yet
		const source = new Memory({ initialData, pk: 'id', schema: null });

		// Add more data before connection
		source.add({ userId: 3, id: 4, amount: 1000 });

		const conn = source.connect();
		const rowComparator = (a: Row, b: Row) => a.id - b.id;
		const groupBy = new GroupByOperator(conn, ['userId'], rowComparator);
		const avg = new AvgGroupByOperator(groupBy, 'amount');
		const view = new View(avg, (a, b) => a.userId - b.userId);

		// Should reflect all data immediately
		expect(view.materialize()).toStrictEqual([
			{ userId: 1, avg: 15 }, // (10 + 20) / 2 = 15
			{ userId: 2, avg: 100 },
			{ userId: 3, avg: 1000 }
		]);
	});

	it('handles oscillation (add/remove/add)', () => {
		type Row = { userId: number; id: number; amount: number };
		const source = new Memory<Row>({ initialData: [], pk: 'id', schema: null });
		const conn = source.connect() as any;
		const rowComparator = (a: Row, b: Row) => a.id - b.id;

		const groupBy = new GroupByOperator(conn, ['userId'], rowComparator);
		const avg = new AvgGroupByOperator(groupBy, 'amount');
		const view = new View(avg, (a, b) => a.userId - b.userId);

		expect(view.materialize()).toStrictEqual([]);
		// Add
		source.add({ userId: 1, id: 1, amount: 10 });
		expect(view.materialize()).toStrictEqual([{ userId: 1, avg: 10 }]);

		// Remove (group disappears)
		source.remove({ userId: 1, id: 1, amount: 10 });
		expect(view.materialize()).toStrictEqual([]);

		// Add again (group reappears)
		source.add({ userId: 1, id: 1, amount: 20 });
		expect(view.materialize()).toStrictEqual([{ userId: 1, avg: 20 }]);
	});

	it('handles decimal averages correctly', () => {
		type Row = { userId: number; id: number; amount: number };
		const initialData: Row[] = [
			{ userId: 1, id: 1, amount: 10 },
			{ userId: 1, id: 2, amount: 15 },
			{ userId: 1, id: 3, amount: 20 }
		];

		const source = new Memory({ initialData, pk: 'id', schema: null });
		const conn = source.connect() as any;
		const rowComparator = (a: Row, b: Row) => a.id - b.id;

		const groupBy = new GroupByOperator(conn, ['userId'], rowComparator);
		const avg = new AvgGroupByOperator(groupBy, 'amount');
		const view = new View(avg, (a, b) => a.userId - b.userId);

		// Average should be (10 + 15 + 20) / 3 = 15
		expect(view.materialize()).toStrictEqual([{ userId: 1, avg: 15 }]);

		// Add row that makes a non-integer average
		source.add({ userId: 1, id: 4, amount: 17 });
		// (10 + 15 + 20 + 17) / 4 = 15.5
		expect(view.materialize()).toStrictEqual([{ userId: 1, avg: 15.5 }]);
	});
	it('handles negatives and zeroes', () => {
		type Row = { userId: number; id: number; amount: number };
		const initialData: Row[] = [
			{ userId: 1, id: 1, amount: -10 },
			{ userId: 1, id: 2, amount: -20 },
			{ userId: 2, id: 3, amount: 100 }
		];

		const source = new Memory({ initialData, pk: 'id', schema: null });

		// Add more data before connection
		source.add({ userId: 2, id: 4, amount: 200 });

		const conn = source.connect() as any;
		const rowComparator = (a: Row, b: Row) => a.id - b.id;
		const groupBy = new GroupByOperator(conn, ['userId'], rowComparator);
		const avg = new AvgGroupByOperator(groupBy, 'amount');
		const view = new View(avg, (a, b) => a.userId - b.userId);

		// Initial: userId 1 has avg of (-10 + -20) / 2 = -15
		//          userId 2 has avg of (100 + 200) / 2 = 150
		expect(view.materialize(), 'initial avg with negatives').toStrictEqual([
			{ userId: 1, avg: -15 },
			{ userId: 2, avg: 150 }
		]);

		// Add zero to userId 1
		source.add({ userId: 1, id: 5, amount: 0 });
		// (-10 + -20 + 0) / 3 = -10
		expect(view.materialize(), 'after adding zero').toStrictEqual([
			{ userId: 1, avg: -10 },
			{ userId: 2, avg: 150 }
		]);

		// Add positive to userId 1 to bring average closer to zero
		source.add({ userId: 1, id: 6, amount: 30 });
		// (-10 + -20 + 0 + 30) / 4 = 0
		expect(view.materialize(), 'average reaches exactly zero').toStrictEqual([
			{ userId: 1, avg: 0 },
			{ userId: 2, avg: 150 }
		]);

		// Add another positive to make average positive
		source.add({ userId: 1, id: 7, amount: 50 });
		// (-10 + -20 + 0 + 30 + 50) / 5 = 10
		expect(view.materialize(), 'average becomes positive').toStrictEqual([
			{ userId: 1, avg: 10 },
			{ userId: 2, avg: 150 }
		]);

		// Remove the positive values to make average negative again
		source.remove({ userId: 1, id: 6, amount: 30 });
		source.remove({ userId: 1, id: 7, amount: 50 });
		// (-10 + -20 + 0) / 3 = -10
		expect(view.materialize(), 'back to negative average').toStrictEqual([
			{ userId: 1, avg: -10 },
			{ userId: 2, avg: 150 }
		]);

		// Create new group with only zero
		source.add({ userId: 3, id: 8, amount: 0 });
		expect(view.materialize(), 'new group with zero value').toStrictEqual([
			{ userId: 1, avg: -10 },
			{ userId: 2, avg: 150 },
			{ userId: 3, avg: 0 }
		]);

		// Add negative to the zero group
		source.add({ userId: 3, id: 9, amount: -50 });
		// (0 + -50) / 2 = -25
		expect(view.materialize(), 'zero group becomes negative').toStrictEqual([
			{ userId: 1, avg: -10 },
			{ userId: 2, avg: 150 },
			{ userId: 3, avg: -25 }
		]);
	});
});
