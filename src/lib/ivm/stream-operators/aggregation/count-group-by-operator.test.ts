import { describe, expect, it } from 'vitest';
import { Memory } from '../../sources/memory.ts';
import { CountGroupByOperator } from './count-group-by-operator.ts';
import { GroupByOperator } from '../group-by-operator.ts';
import { View } from '../../sinks/view.ts';

describe('CountGroupByOperator', () => {
	it('counts rows per group (COUNT with GROUP BY)', () => {
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
		const count = new CountGroupByOperator(groupBy);
		const view = new View(count, (a, b) => a.userId - b.userId);

		// Initial counts per group
		expect(view.materialize(), 'initial count is correct').toStrictEqual([
			{ userId: 1, count: 2 },
			{ userId: 2, count: 1 },
			{ userId: 3, count: 1 }
		]);

		// Add row to existing group
		source.add({ userId: 1, id: 5, amount: 50 });
		expect(view.materialize(), 'after adding to userId 1').toStrictEqual([
			{ userId: 1, count: 3 },
			{ userId: 2, count: 1 },
			{ userId: 3, count: 1 }
		]);

		// Add row to new group
		source.add({ userId: 4, id: 6, amount: 60 });
		expect(view.materialize(), 'after adding to userId 4').toStrictEqual([
			{ userId: 1, count: 3 },
			{ userId: 2, count: 1 },
			{ userId: 3, count: 1 },
			{ userId: 4, count: 1 }
		]);

		// Remove row from group
		source.remove({ userId: 1, id: 1, amount: 10 });
		expect(view.materialize(), 'after removing id 1').toStrictEqual([
			{ userId: 1, count: 2 },
			{ userId: 2, count: 1 },
			{ userId: 3, count: 1 },
			{ userId: 4, count: 1 }
		]);

		// // Remove last row from group (group should disappear)
		source.remove({ userId: 3, id: 4, amount: 40 });
		expect(view.materialize()).toStrictEqual([
			{ userId: 1, count: 2 },
			{ userId: 2, count: 1 },
			{ userId: 4, count: 1 }
		]);
	});

	it('counts rows per group (COUNT with GROUP BY) when subscribed to View', () => {
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
		const count = new CountGroupByOperator(groupBy as any);
		const view = new View(count, (a, b) => a.userId - b.userId);

		// Initial counts per group
		expect(view.materialize(), 'initial count is correct').toStrictEqual([
			{ userId: 1, count: 2 },
			{ userId: 2, count: 1 },
			{ userId: 3, count: 1 }
		]);
		let results;

		view.subscribe((res) => {
			results = res;
		});

		expect(results).toStrictEqual([
			{ userId: 1, count: 2 },
			{ userId: 2, count: 1 },
			{ userId: 3, count: 1 }
		]);

		// Add row to existing group
		source.add({ userId: 1, id: 5, amount: 50 });
		expect(results, 'after adding to userId 1').toStrictEqual([
			{ userId: 1, count: 3 },
			{ userId: 2, count: 1 },
			{ userId: 3, count: 1 }
		]);

		// Add row to new group
		source.add({ userId: 4, id: 6, amount: 60 });
		expect(results, 'after adding to userId 4').toStrictEqual([
			{ userId: 1, count: 3 },
			{ userId: 2, count: 1 },
			{ userId: 3, count: 1 },
			{ userId: 4, count: 1 }
		]);

		// Remove row from group
		source.remove({ userId: 1, id: 1, amount: 10 });
		expect(results, 'after removing id 1').toStrictEqual([
			{ userId: 1, count: 2 },
			{ userId: 2, count: 1 },
			{ userId: 3, count: 1 },
			{ userId: 4, count: 1 }
		]);

		// // Remove last row from group (group should disappear)
		source.remove({ userId: 3, id: 4, amount: 40 });
		expect(results).toStrictEqual([
			{ userId: 1, count: 2 },
			{ userId: 2, count: 1 },
			{ userId: 4, count: 1 }
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
		const count = new CountGroupByOperator(groupBy as any);
		const view = new View(count, (a, b) => a.userId - b.userId);

		expect(view.materialize()).toStrictEqual([
			{ userId: 1, count: 1 },
			{ userId: 2, count: 1 }
		]);

		// Move id:1 from userId:1 to userId:2
		source.update({ userId: 1, id: 1, amount: 10 }, { userId: 2 });

		expect(view.materialize()).toStrictEqual([
			{ userId: 2, count: 2 }
			// userId:1 should be gone because count dropped to 0
		]);
	});

	it('handles row updates NOT changing the grouping key', () => {
		type Row = { userId: number; id: number; amount: number };
		const initialData: Row[] = [{ userId: 1, id: 1, amount: 10 }];

		const source = new Memory({ initialData, pk: 'id', schema: null });
		const conn = source.connect() as any;
		const rowComparator = (a: Row, b: Row) => a.id - b.id;

		const groupBy = new GroupByOperator(conn, ['userId'], rowComparator);
		const count = new CountGroupByOperator(groupBy as any);
		const view = new View(count, (a, b) => a.userId - b.userId);

		expect(view.materialize()).toStrictEqual([{ userId: 1, count: 1 }]);

		// Update amount only
		source.update({ userId: 1, id: 1, amount: 10 }, { amount: 50 });

		expect(view.materialize()).toStrictEqual([{ userId: 1, count: 1 }]);
	});

	it('supports grouping by multiple keys', () => {
		type Row = { region: string; type: string; id: number };
		const initialData: Row[] = [
			{ region: 'US', type: 'A', id: 1 },
			{ region: 'US', type: 'A', id: 2 },
			{ region: 'US', type: 'B', id: 3 },
			{ region: 'EU', type: 'A', id: 4 }
		];

		const source = new Memory({ initialData, pk: 'id', schema: null });
		const conn = source.connect() as any;
		const rowComparator = (a: Row, b: Row) => a.id - b.id;

		const groupBy = new GroupByOperator(conn, ['region', 'type'], rowComparator);
		const count = new CountGroupByOperator(groupBy as any);
		const view = new View(count, (a, b) => {
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
			{ region: 'EU', type: 'A', count: 1 },
			{ region: 'US', type: 'A', count: 2 },
			{ region: 'US', type: 'B', count: 1 }
		]);

		// Add another US/B
		source.add({ region: 'US', type: 'B', id: 5 });

		expect(view.materialize()).toStrictEqual([
			{ region: 'EU', type: 'A', count: 1 },
			{ region: 'US', type: 'A', count: 2 },
			{ region: 'US', type: 'B', count: 2 }
		]);
	});

	it('correctly initializes from pre-populated source', () => {
		type Row = { userId: number; id: number };
		const initialData: Row[] = [
			{ userId: 1, id: 1 },
			{ userId: 1, id: 2 },
			{ userId: 2, id: 3 }
		];
		// Setup source but don't connect yet
		const source = new Memory({ initialData, pk: 'id', schema: null });

		// Add more data before connection
		source.add({ userId: 3, id: 4 });

		const conn = source.connect() as any;
		const rowComparator = (a: Row, b: Row) => a.id - b.id;
		const groupBy = new GroupByOperator(conn, ['userId'], rowComparator);
		const count = new CountGroupByOperator(groupBy as any);
		const view = new View(count, (a, b) => a.userId - b.userId);

		// Should reflect all data immediately
		expect(view.materialize()).toStrictEqual([
			{ userId: 1, count: 2 },
			{ userId: 2, count: 1 },
			{ userId: 3, count: 1 }
		]);
	});

	it('handles oscillation (add/remove/add)', () => {
		type Row = { userId: number; id: number };
		const source = new Memory<Row>({ initialData: [], pk: 'id', schema: null });
		const conn = source.connect() as any;
		const rowComparator = (a: Row, b: Row) => a.id - b.id;

		const groupBy = new GroupByOperator(conn, ['userId'], rowComparator);
		const count = new CountGroupByOperator(groupBy as any);
		const view = new View(count, (a, b) => a.userId - b.userId);

		// Add
		source.add({ userId: 1, id: 1 });
		expect(view.materialize()).toStrictEqual([{ userId: 1, count: 1 }]);

		// Remove (group disappears)
		source.remove({ userId: 1, id: 1 });
		expect(view.materialize()).toStrictEqual([]);

		// Add again (group reappears)
		source.add({ userId: 1, id: 1 });
		expect(view.materialize()).toStrictEqual([{ userId: 1, count: 1 }]);
	});

	it('handles rows with negative and zero values', () => {
		type Row = { userId: number; id: number; amount: number };
		const initialData: Row[] = [
			{ userId: 1, id: 1, amount: -10 },
			{ userId: 1, id: 2, amount: 0 },
			{ userId: 1, id: 3, amount: -100 },
			{ userId: 2, id: 4, amount: 0 }
		];

		const source = new Memory({ initialData, pk: 'id', schema: null });

		// Add more data before connection
		source.add({ userId: 2, id: 5, amount: -50 });

		const conn = source.connect() as any;
		const rowComparator = (a: Row, b: Row) => a.id - b.id;
		const groupBy = new GroupByOperator(conn, ['userId'], rowComparator);
		const count = new CountGroupByOperator(groupBy as any);
		const view = new View(count, (a, b) => a.userId - b.userId);

		// Count should be based on number of rows, not their values
		expect(view.materialize(), 'initial count with negative/zero values').toStrictEqual([
			{ userId: 1, count: 3 }, // 3 rows regardless of values
			{ userId: 2, count: 2 } // 2 rows regardless of values
		]);

		// Add row with zero value
		source.add({ userId: 1, id: 6, amount: 0 });
		expect(view.materialize(), 'after adding row with zero').toStrictEqual([
			{ userId: 1, count: 4 },
			{ userId: 2, count: 2 }
		]);

		// Add row with negative value
		source.add({ userId: 3, id: 7, amount: -999 });
		expect(view.materialize(), 'new group with negative value').toStrictEqual([
			{ userId: 1, count: 4 },
			{ userId: 2, count: 2 },
			{ userId: 3, count: 1 }
		]);

		// Remove row with zero value
		source.remove({ userId: 1, id: 2, amount: 0 });
		expect(view.materialize(), 'after removing row with zero').toStrictEqual([
			{ userId: 1, count: 3 },
			{ userId: 2, count: 2 },
			{ userId: 3, count: 1 }
		]);

		// Remove all rows from a group
		source.remove({ userId: 3, id: 7, amount: -999 });
		expect(view.materialize(), 'group disappears when count reaches 0').toStrictEqual([
			{ userId: 1, count: 3 },
			{ userId: 2, count: 2 }
		]);
	});
});
