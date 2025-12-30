import { describe, expect, it } from 'vitest';
import { defaultComparator, Memory } from '../../sources/memory.ts';
import { View } from '../../sinks/view.ts';
import { SumOperator } from './sum-operator.ts';

describe('SumOperator', () => {
	type Row = { id: number; amount: number };
	const initialData: Row[] = [
		{ id: 1, amount: 10 },
		{ id: 2, amount: 20 },
		{ id: 3, amount: 30 }
	];

	it('sums all rows (SUM(*))', () => {
		const source = new Memory({ initialData, pk: 'id', schema: null });
		const conn = source.connect();
		const sum = new SumOperator(conn, { column: 'amount' });
		const view = new View(sum, defaultComparator('sum'));

		expect(view.materialize()).toStrictEqual([{ sum: 60 }]);
		source.add({ id: 4, amount: 40 });
		expect(view.currentState()).toStrictEqual([{ sum: 100 }]);
		source.remove({ id: 1, amount: 10 });
		expect(view.currentState()).toStrictEqual([{ sum: 90 }]);
		expect(view.materialize()).toStrictEqual([{ sum: 90 }]);
	});

	it('sums all rows (SUM(*)) with subscribe', () => {
		const source = new Memory({ initialData, pk: 'id', schema: null });
		const conn = source.connect();
		const sum = new SumOperator(conn, { column: 'amount' });
		const view = new View(sum, defaultComparator('sum'));
		let result = view.materialize();
		view.subscribe((r) => (result = r));
		expect(result).toStrictEqual([{ sum: 60 }]);
		source.add({ id: 4, amount: 40 });
		expect(result).toStrictEqual([{ sum: 100 }]);
		source.remove({ id: 1, amount: 10 });
		expect(result).toStrictEqual([{ sum: 90 }]);
	});
});
