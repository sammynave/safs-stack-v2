import { describe, expect, it } from 'vitest';
import { defaultComparator, Memory } from '../../sources/memory.ts';
import { View } from '../../sinks/view.ts';
import { ArrayAggOperator } from './array-agg-operator.ts';

describe('ArrayAggOperator', () => {
	type Row = { id: number; userId: number; tag: string };
	const initialData: Row[] = [
		{ id: 1, userId: 1, tag: 'butt' },
		{ id: 2, userId: 1, tag: 'barf' },
		{ id: 3, userId: 2, tag: 'barf' }
	];

	// Custom comparator for array results - since there's only one result, just return 0
	const arrayComparator = (a: { arrayAgg: string[] }, b: { arrayAgg: string[] }) => {
		return 0; // Single result aggregation, no ordering needed
	};

	it('aggregates tags into array (ARRAY_AGG(tag))', () => {
		const source = new Memory({ initialData, pk: 'id', schema: null });
		const conn = source.connect();
		const agg = new ArrayAggOperator(conn, { column: 'tag' });
		const view = new View(agg, arrayComparator);

		expect(view.materialize()).toStrictEqual([{ arrayAgg: ['butt', 'barf', 'barf'] }]);
		source.add({ id: 4, userId: 2, tag: 'stank boy' });
		expect(view.materialize()).toStrictEqual([{ arrayAgg: ['butt', 'barf', 'barf', 'stank boy'] }]);
		source.remove({ id: 1, userId: 1, tag: 'butt' });
		expect(view.materialize()).toStrictEqual([{ arrayAgg: ['barf', 'barf', 'stank boy'] }]);
		source.add({ id: 1, userId: 1, tag: 'butt' });
		expect(view.materialize()).toStrictEqual([{ arrayAgg: ['barf', 'barf', 'stank boy', 'butt'] }]);
	});

	it('returns empty array for empty source', () => {
		const source = new Memory({ initialData: [], pk: 'id', schema: null });
		const conn = source.connect();
		const agg = new ArrayAggOperator(conn, { column: 'tag' });
		const view = new View(agg, arrayComparator);

		expect(view.materialize()).toStrictEqual([{ arrayAgg: [] }]);
	});

	it('filters out null and undefined values', () => {
		const initialData = [
			{ id: 1, tag: 'valid' },
			{ id: 2, tag: null },
			{ id: 3, tag: undefined },
			{ id: 4, tag: 'another' }
		];
		const source = new Memory({ initialData, pk: 'id', schema: null });
		const conn = source.connect();
		const agg = new ArrayAggOperator(conn, { column: 'tag' });
		const view = new View(agg, arrayComparator);

		// Should only include 'valid' and 'another'
		expect(view.materialize()).toStrictEqual([{ arrayAgg: ['valid', 'another'] }]);
	});
});
