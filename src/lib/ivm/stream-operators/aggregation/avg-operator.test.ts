import { describe, expect, it } from 'vitest';
import { Memory } from '../../sources/memory.ts';
import { View } from '../../sinks/view.ts';
import { AvgOperator } from './avg-operator.ts';

describe('AvgOperator', () => {
	type Row = { id: number; amount: number };
	const initialData: Row[] = [
		{ id: 1, amount: 10 },
		{ id: 2, amount: 20 },
		{ id: 3, amount: 30 }
	];

	it('sums all rows (AVG(column))', () => {
		const source = new Memory({ initialData, pk: 'id', schema: null });
		const avgOp = new AvgOperator(source.connect(), { column: 'amount' });
		// NOTE: this is interesting, is (a,b) => 0 better than defaultComparator('amount')?
		const view = new View(avgOp, (a, b) => 0);

		expect(view.materialize()).toStrictEqual([{ avg: 20 }]);
		source.add({ id: 4, amount: 40 });
		expect(view.currentState()).toStrictEqual([{ avg: 25 }]);
		source.add({ id: 5, amount: 50 });
		expect(view.currentState()).toStrictEqual([{ avg: 30 }]);
		source.remove({ id: 5, amount: 50 });
		expect(view.currentState()).toStrictEqual([{ avg: 25 }]);
	});

	it('sums all rows (SUM(column)) with subscribe', () => {
		const source = new Memory({ initialData, pk: 'id', schema: null });
		const avgOp = new AvgOperator(source.connect(), { column: 'amount' });
		const view = new View(avgOp, (a, b) => 0);
		let result = view.materialize();
		view.subscribe((r) => (result = r));

		expect(result).toStrictEqual([{ avg: 20 }]);
		source.add({ id: 4, amount: 40 });
		expect(result).toStrictEqual([{ avg: 25 }]);
		source.add({ id: 5, amount: 50 });
		expect(result).toStrictEqual([{ avg: 30 }]);
		source.remove({ id: 5, amount: 50 });
		expect(result).toStrictEqual([{ avg: 25 }]);
	});
});
