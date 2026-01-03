import { describe, expect, it } from 'vitest';
import { Memory } from '../../sources/memory.ts';
import { MaxGroupByOperator } from './max-group-by-operator.ts';
import { GroupByOperator } from '../group-by-operator.ts';
import { View } from '../../sinks/view.ts';

describe('MaxGroupByOperator', () => {
	it('maxes rows per group (MAX with GROUP BY)', () => {
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
		const max = new MaxGroupByOperator(groupBy, 'amount');
		const view = new View(max, (a, b) => a.userId - b.userId);

		// Initial maxes per group
		expect(view.materialize(), 'initial max is correct').toStrictEqual([
			{ userId: 1, max: 20 },
			{ userId: 2, max: 30 },
			{ userId: 3, max: 40 }
		]);

		// Add row to existing group
		source.add({ userId: 1, id: 5, amount: 50 });
		expect(view.materialize(), 'after adding to userId 1').toStrictEqual([
			{ userId: 1, max: 50 },
			{ userId: 2, max: 30 },
			{ userId: 3, max: 40 }
		]);

		// Add row to new group
		source.add({ userId: 4, id: 6, amount: 60 });
		expect(view.materialize(), 'after adding to userId 4').toStrictEqual([
			{ userId: 1, max: 50 },
			{ userId: 2, max: 30 },
			{ userId: 3, max: 40 },
			{ userId: 4, max: 60 }
		]);

		// Remove row from group
		source.remove({ userId: 1, id: 1, amount: 10 });
		expect(view.materialize(), 'after removing id 1').toStrictEqual([
			{ userId: 1, max: 50 },
			{ userId: 2, max: 30 },
			{ userId: 3, max: 40 },
			{ userId: 4, max: 60 }
		]);
		// Remove last row from group (group should disappear)
		source.remove({ userId: 3, id: 4, amount: 40 });
		expect(view.materialize()).toStrictEqual([
			{ userId: 1, max: 50 },
			{ userId: 2, max: 30 },
			{ userId: 4, max: 60 }
		]);
	});

	it('maxes rows per group (MAX with GROUP BY) when subscribed to View', () => {
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
		const max = new MaxGroupByOperator(groupBy, 'amount');
		const view = new View(max, (a, b) => a.userId - b.userId);

		// Initial maxes per group
		expect(view.materialize(), 'initial max is correct').toStrictEqual([
			{ userId: 1, max: 20 },
			{ userId: 2, max: 30 },
			{ userId: 3, max: 40 }
		]);
		let results;

		view.subscribe((res) => {
			results = res;
		});

		expect(results).toStrictEqual([
			{ userId: 1, max: 20 },
			{ userId: 2, max: 30 },
			{ userId: 3, max: 40 }
		]);

		// Add row to existing group
		source.add({ userId: 1, id: 5, amount: 50 });
		expect(results, 'after adding to userId 1').toStrictEqual([
			{ userId: 1, max: 50 },
			{ userId: 2, max: 30 },
			{ userId: 3, max: 40 }
		]);

		// Add row to new group
		source.add({ userId: 4, id: 6, amount: 60 });
		expect(results, 'after adding to userId 4').toStrictEqual([
			{ userId: 1, max: 50 },
			{ userId: 2, max: 30 },
			{ userId: 3, max: 40 },
			{ userId: 4, max: 60 }
		]);

		// Remove row from group
		source.remove({ userId: 1, id: 1, amount: 10 });
		expect(results, 'after removing id 1').toStrictEqual([
			{ userId: 1, max: 50 },
			{ userId: 2, max: 30 },
			{ userId: 3, max: 40 },
			{ userId: 4, max: 60 }
		]);

		// // Remove last row from group (group should disappear)
		source.remove({ userId: 3, id: 4, amount: 40 });
		expect(results).toStrictEqual([
			{ userId: 1, max: 50 },
			{ userId: 2, max: 30 },
			{ userId: 4, max: 60 }
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
		const max = new MaxGroupByOperator(groupBy, 'amount');
		const view = new View(max, (a, b) => a.userId - b.userId);

		expect(view.materialize()).toStrictEqual([
			{ userId: 1, max: 10 },
			{ userId: 2, max: 20 }
		]);

		// Move id:1 from userId:1 to userId:2
		source.update({ userId: 1, id: 1, amount: 10 }, { userId: 2 });

		expect(view.materialize()).toStrictEqual([
			{ userId: 2, max: 20 }
			// userId:1 should be gone because max dropped to 0
		]);
	});

	it('handles row updates NOT changing the grouping key', () => {
		type Row = { userId: number; id: number; amount: number };
		const initialData: Row[] = [{ userId: 1, id: 1, amount: 10 }];

		const source = new Memory({ initialData, pk: 'id', schema: null });
		const conn = source.connect() as any;
		const rowComparator = (a: Row, b: Row) => a.id - b.id;

		const groupBy = new GroupByOperator(conn, ['userId'], rowComparator);
		const max = new MaxGroupByOperator(groupBy, 'amount');
		const view = new View(max, (a, b) => a.userId - b.userId);

		expect(view.materialize()).toStrictEqual([{ userId: 1, max: 10 }]);

		// Update amount only
		source.update({ userId: 1, id: 1, amount: 10 }, { amount: 50 });

		expect(view.materialize()).toStrictEqual([{ userId: 1, max: 50 }]);
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
		const max = new MaxGroupByOperator(groupBy, 'amount');
		const view = new View(max, (a, b) => {
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

		expect(view.materialize(), 'initial pull is correct').toStrictEqual([
			{ region: 'EU', type: 'A', max: 1 },
			{ region: 'US', type: 'A', max: 30 },
			{ region: 'US', type: 'B', max: 10 }
		]);

		// Add another US/B
		source.add({ region: 'US', type: 'B', id: 5, amount: 80 });

		expect(view.materialize(), 'after first add is correct').toStrictEqual([
			{ region: 'EU', type: 'A', max: 1 },
			{ region: 'US', type: 'A', max: 30 },
			{ region: 'US', type: 'B', max: 80 }
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
		const max = new MaxGroupByOperator(groupBy, 'amount');
		const view = new View(max, (a, b) => a.userId - b.userId);

		// Should reflect all data immediately
		expect(view.materialize()).toStrictEqual([
			{ userId: 1, max: 10 },
			{ userId: 2, max: 100 },
			{ userId: 3, max: 1000 }
		]);
	});

	it('handles oscillation (add/remove/add)', () => {
		type Row = { userId: number; id: number; amount: 10 };
		const source = new Memory<Row>({ initialData: [], pk: 'id', schema: null });
		const conn = source.connect() as any;
		const rowComparator = (a: Row, b: Row) => a.id - b.id;

		const groupBy = new GroupByOperator(conn, ['userId'], rowComparator);
		const max = new MaxGroupByOperator(groupBy, 'amount');
		const view = new View(max, (a, b) => a.userId - b.userId);

		expect(view.materialize()).toStrictEqual([]);
		// Add
		source.add({ userId: 1, id: 1, amount: 10 });
		expect(view.materialize()).toStrictEqual([{ userId: 1, max: 10 }]);

		// Remove (group disappears)
		source.remove({ userId: 1, id: 1, amount: 10 });
		expect(view.materialize()).toStrictEqual([]);

		// Add again (group reappears)
		source.add({ userId: 1, id: 1, amount: 1 });
		expect(view.materialize()).toStrictEqual([{ userId: 1, max: 1 }]);
	});
	it('handles negatives and zeroes', () => {
		type Row = { userId: number; id: number; amount: number };
		const initialData: Row[] = [
			{ userId: 1, id: 1, amount: -10 },
			{ userId: 1, id: 2, amount: -100 },
			{ userId: 2, id: 3, amount: 100 }
		];
		// Setup source but don't connect yet
		const source = new Memory({ initialData, pk: 'id', schema: null });

		// Add more data before connection
		source.add({ userId: 2, id: 4, amount: 1000 });

		const conn = source.connect();
		const rowComparator = (a: Row, b: Row) => a.id - b.id;
		const groupBy = new GroupByOperator(conn, ['userId'], rowComparator);
		const max = new MaxGroupByOperator(groupBy, 'amount');
		const view = new View(max, (a, b) => a.userId - b.userId);

		expect(view.materialize(), 'initial is correct').toStrictEqual([
			{ userId: 1, max: -10 },
			{ userId: 2, max: 1000 }
		]);

		source.add({ userId: 1, id: 6, amount: -1 });
		expect(view.materialize(), 'negative is the minimum').toStrictEqual([
			{ userId: 1, max: -1 },
			{ userId: 2, max: 1000 }
		]);

		source.add({ userId: 1, id: 5, amount: 0 });
		expect(view.materialize(), 'zero is the minimum').toStrictEqual([
			{ userId: 1, max: 0 },
			{ userId: 2, max: 1000 }
		]);
	});
});
