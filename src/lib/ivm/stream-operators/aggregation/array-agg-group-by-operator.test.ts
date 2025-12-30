import { describe, expect, it } from 'vitest';
import { Memory } from '../../sources/memory.ts';
import { ArrayAggGroupByOperator } from './array-agg-group-by-operator.ts';
import { GroupByOperator } from '../group-by-operator.ts';
import { View } from '../../sinks/view.ts';

describe('ArrayAggGroupByOperator', () => {
	it('aggregates strings into arrays per group (ARRAY_AGG with GROUP BY)', () => {
		type Row = { userId: number; id: number; tag: string };
		const initialData: Row[] = [
			{ userId: 1, id: 1, tag: 'cool' },
			{ userId: 1, id: 2, tag: 'awesome' },
			{ userId: 2, id: 3, tag: 'neat' },
			{ userId: 3, id: 4, tag: 'rad' }
		];

		const source = new Memory({ initialData, pk: 'id', schema: null });
		const conn = source.connect() as any;

		const rowComparator = (a: Row, b: Row) => {
			if (a.id < b.id) return -1;
			if (a.id > b.id) return 1;
			return 0;
		};

		const groupBy = new GroupByOperator(conn, ['userId'], rowComparator);
		const arrayAgg = new ArrayAggGroupByOperator(groupBy, 'tag');
		const view = new View(arrayAgg, (a, b) => a.userId - b.userId);

		// Initial arrays per group
		expect(view.materialize(), 'initial arrayAgg is correct').toStrictEqual([
			{ userId: 1, arrayAgg: ['cool', 'awesome'] },
			{ userId: 2, arrayAgg: ['neat'] },
			{ userId: 3, arrayAgg: ['rad'] }
		]);

		// Add row to existing group
		source.add({ userId: 1, id: 5, tag: 'super' });
		expect(view.materialize(), 'after adding to userId 1').toStrictEqual([
			{ userId: 1, arrayAgg: ['cool', 'awesome', 'super'] },
			{ userId: 2, arrayAgg: ['neat'] },
			{ userId: 3, arrayAgg: ['rad'] }
		]);

		// Add row to new group
		source.add({ userId: 4, id: 6, tag: 'fantastic' });
		expect(view.materialize(), 'after adding to userId 4').toStrictEqual([
			{ userId: 1, arrayAgg: ['cool', 'awesome', 'super'] },
			{ userId: 2, arrayAgg: ['neat'] },
			{ userId: 3, arrayAgg: ['rad'] },
			{ userId: 4, arrayAgg: ['fantastic'] }
		]);

		// Remove row from group
		source.remove({ userId: 1, id: 1, tag: 'cool' });
		expect(view.materialize(), 'after removing id 1').toStrictEqual([
			{ userId: 1, arrayAgg: ['awesome', 'super'] },
			{ userId: 2, arrayAgg: ['neat'] },
			{ userId: 3, arrayAgg: ['rad'] },
			{ userId: 4, arrayAgg: ['fantastic'] }
		]);

		// Remove last row from group (group should disappear)
		source.remove({ userId: 3, id: 4, tag: 'rad' });
		expect(view.materialize()).toStrictEqual([
			{ userId: 1, arrayAgg: ['awesome', 'super'] },
			{ userId: 2, arrayAgg: ['neat'] },
			{ userId: 4, arrayAgg: ['fantastic'] }
		]);
	});

	it('handles duplicate values within a group', () => {
		type Row = { userId: number; id: number; tag: string };
		const initialData: Row[] = [
			{ userId: 1, id: 1, tag: 'duplicate' },
			{ userId: 1, id: 2, tag: 'duplicate' },
			{ userId: 1, id: 3, tag: 'unique' }
		];

		const source = new Memory({ initialData, pk: 'id', schema: null });
		const conn = source.connect() as any;
		const rowComparator = (a: Row, b: Row) => a.id - b.id;

		const groupBy = new GroupByOperator(conn, ['userId'], rowComparator);
		const arrayAgg = new ArrayAggGroupByOperator(groupBy, 'tag');
		const view = new View(arrayAgg, (a, b) => a.userId - b.userId);

		expect(view.materialize()).toStrictEqual([
			{ userId: 1, arrayAgg: ['duplicate', 'duplicate', 'unique'] }
		]);

		// Remove one duplicate
		source.remove({ userId: 1, id: 1, tag: 'duplicate' });
		expect(view.materialize()).toStrictEqual([{ userId: 1, arrayAgg: ['duplicate', 'unique'] }]);
	});

	it('filters out null and undefined values', () => {
		type Row = { userId: number; id: number; tag: string | null };
		const initialData: Row[] = [
			{ userId: 1, id: 1, tag: 'valid' },
			{ userId: 1, id: 2, tag: null },
			{ userId: 1, id: 3, tag: 'another' }
		];

		const source = new Memory({ initialData, pk: 'id', schema: null });
		const conn = source.connect() as any;
		const rowComparator = (a: Row, b: Row) => a.id - b.id;

		const groupBy = new GroupByOperator(conn, ['userId'], rowComparator);
		const arrayAgg = new ArrayAggGroupByOperator(groupBy, 'tag');
		const view = new View(arrayAgg, (a, b) => a.userId - b.userId);

		// Should only include non-null values
		expect(view.materialize()).toStrictEqual([{ userId: 1, arrayAgg: ['valid', 'another'] }]);
	});

	it('works with subscribe pattern', () => {
		type Row = { userId: number; id: number; tag: string };
		const initialData: Row[] = [
			{ userId: 1, id: 1, tag: 'first' },
			{ userId: 2, id: 2, tag: 'second' }
		];

		const source = new Memory({ initialData, pk: 'id', schema: null });
		const conn = source.connect() as any;
		const rowComparator = (a: Row, b: Row) => a.id - b.id;

		const groupBy = new GroupByOperator(conn, ['userId'], rowComparator);
		const arrayAgg = new ArrayAggGroupByOperator(groupBy, 'tag');
		const view = new View(arrayAgg, (a, b) => a.userId - b.userId);

		let results = view.materialize();
		view.subscribe((res) => {
			results = res;
		});

		expect(results).toStrictEqual([
			{ userId: 1, arrayAgg: ['first'] },
			{ userId: 2, arrayAgg: ['second'] }
		]);

		source.add({ userId: 1, id: 3, tag: 'third' });
		expect(results).toStrictEqual([
			{ userId: 1, arrayAgg: ['first', 'third'] },
			{ userId: 2, arrayAgg: ['second'] }
		]);
	});

	it('handles row updates changing the grouping key', () => {
		type Row = { userId: number; id: number; tag: string };
		const initialData: Row[] = [
			{ userId: 1, id: 1, tag: 'tag1' },
			{ userId: 2, id: 2, tag: 'tag2' }
		];

		const source = new Memory({ initialData, pk: 'id', schema: null });
		const conn = source.connect() as any;
		const rowComparator = (a: Row, b: Row) => a.id - b.id;

		const groupBy = new GroupByOperator(conn, ['userId'], rowComparator);
		const arrayAgg = new ArrayAggGroupByOperator(groupBy, 'tag');
		const view = new View(arrayAgg, (a, b) => a.userId - b.userId);

		expect(view.materialize()).toStrictEqual([
			{ userId: 1, arrayAgg: ['tag1'] },
			{ userId: 2, arrayAgg: ['tag2'] }
		]);

		// Move id:1 from userId:1 to userId:2
		source.update({ userId: 1, id: 1, tag: 'tag1' }, { userId: 2 });

		expect(view.materialize()).toStrictEqual([
			{ userId: 2, arrayAgg: ['tag1', 'tag2'] }
			// userId:1 should be gone because it has no rows
			// Note: array order is by id (1, 2) due to BTree iteration order
		]);
	});

	it('handles row updates NOT changing the grouping key', () => {
		type Row = { userId: number; id: number; tag: string };
		const initialData: Row[] = [{ userId: 1, id: 1, tag: 'old' }];

		const source = new Memory({ initialData, pk: 'id', schema: null });
		const conn = source.connect() as any;
		const rowComparator = (a: Row, b: Row) => a.id - b.id;

		const groupBy = new GroupByOperator(conn, ['userId'], rowComparator);
		const arrayAgg = new ArrayAggGroupByOperator(groupBy, 'tag');
		const view = new View(arrayAgg, (a, b) => a.userId - b.userId);

		expect(view.materialize()).toStrictEqual([{ userId: 1, arrayAgg: ['old'] }]);

		// Update tag only
		source.update({ userId: 1, id: 1, tag: 'old' }, { tag: 'new' });

		expect(view.materialize()).toStrictEqual([{ userId: 1, arrayAgg: ['new'] }]);
	});

	it('supports grouping by multiple keys', () => {
		type Row = { region: string; type: string; id: number; tag: string };
		const initialData: Row[] = [
			{ region: 'US', type: 'A', id: 1, tag: 'tag1' },
			{ region: 'US', type: 'A', id: 2, tag: 'tag2' },
			{ region: 'US', type: 'B', id: 3, tag: 'tag3' },
			{ region: 'EU', type: 'A', id: 4, tag: 'tag4' }
		];

		const source = new Memory({ initialData, pk: 'id', schema: null });
		const conn = source.connect() as any;
		const rowComparator = (a: Row, b: Row) => a.id - b.id;

		const groupBy = new GroupByOperator(conn, ['region', 'type'], rowComparator);
		const arrayAgg = new ArrayAggGroupByOperator(groupBy, 'tag');
		const view = new View(arrayAgg, (a, b) => {
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
			{ region: 'EU', type: 'A', arrayAgg: ['tag4'] },
			{ region: 'US', type: 'A', arrayAgg: ['tag1', 'tag2'] },
			{ region: 'US', type: 'B', arrayAgg: ['tag3'] }
		]);

		// Add another US/B
		source.add({ region: 'US', type: 'B', id: 5, tag: 'tag5' });

		expect(view.materialize()).toStrictEqual([
			{ region: 'EU', type: 'A', arrayAgg: ['tag4'] },
			{ region: 'US', type: 'A', arrayAgg: ['tag1', 'tag2'] },
			{ region: 'US', type: 'B', arrayAgg: ['tag3', 'tag5'] }
		]);
	});

	it('correctly initializes from pre-populated source', () => {
		type Row = { userId: number; id: number; tag: string };
		const initialData: Row[] = [
			{ userId: 1, id: 1, tag: 'a' },
			{ userId: 1, id: 2, tag: 'b' },
			{ userId: 2, id: 3, tag: 'c' }
		];
		// Setup source but don't connect yet
		const source = new Memory({ initialData, pk: 'id', schema: null });

		// Add more data before connection
		source.add({ userId: 3, id: 4, tag: 'd' });

		const conn = source.connect();
		const rowComparator = (a: Row, b: Row) => a.id - b.id;
		const groupBy = new GroupByOperator(conn, ['userId'], rowComparator);
		const arrayAgg = new ArrayAggGroupByOperator(groupBy, 'tag');
		const view = new View(arrayAgg, (a, b) => a.userId - b.userId);

		// Should reflect all data immediately
		expect(view.materialize()).toStrictEqual([
			{ userId: 1, arrayAgg: ['a', 'b'] },
			{ userId: 2, arrayAgg: ['c'] },
			{ userId: 3, arrayAgg: ['d'] }
		]);
	});

	it('handles oscillation (add/remove/add)', () => {
		type Row = { userId: number; id: number; tag: string };
		const source = new Memory<Row>({ initialData: [], pk: 'id', schema: null });
		const conn = source.connect() as any;
		const rowComparator = (a: Row, b: Row) => a.id - b.id;

		const groupBy = new GroupByOperator(conn, ['userId'], rowComparator);
		const arrayAgg = new ArrayAggGroupByOperator(groupBy, 'tag');
		const view = new View(arrayAgg, (a, b) => a.userId - b.userId);

		expect(view.materialize()).toStrictEqual([]);

		// Add
		source.add({ userId: 1, id: 1, tag: 'first' });
		expect(view.materialize()).toStrictEqual([{ userId: 1, arrayAgg: ['first'] }]);

		// Remove (group disappears)
		source.remove({ userId: 1, id: 1, tag: 'first' });
		expect(view.materialize()).toStrictEqual([]);

		// Add again (group reappears)
		source.add({ userId: 1, id: 1, tag: 'second' });
		expect(view.materialize()).toStrictEqual([{ userId: 1, arrayAgg: ['second'] }]);
	});

	it('handles empty groups correctly', () => {
		type Row = { userId: number; id: number; tag: string };
		const initialData: Row[] = [{ userId: 1, id: 1, tag: 'only' }];

		const source = new Memory({ initialData, pk: 'id', schema: null });
		const conn = source.connect() as any;
		const rowComparator = (a: Row, b: Row) => a.id - b.id;

		const groupBy = new GroupByOperator(conn, ['userId'], rowComparator);
		const arrayAgg = new ArrayAggGroupByOperator(groupBy, 'tag');
		const view = new View(arrayAgg, (a, b) => a.userId - b.userId);

		expect(view.materialize()).toStrictEqual([{ userId: 1, arrayAgg: ['only'] }]);

		// Remove the only row - group should disappear
		source.remove({ userId: 1, id: 1, tag: 'only' });
		expect(view.materialize()).toStrictEqual([]);
	});
});
