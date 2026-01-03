import { describe, expect, it } from 'vitest';
import { defaultComparator, Memory } from '../../sources/memory.ts';
import { View } from '../../sinks/view.ts';
import { MaxOperator } from './max-opertator.ts';

describe('MaxOperator', () => {
	type Row = { id: number; amount: number };
	const initialData: Row[] = [
		{ id: 1, amount: 10 },
		{ id: 2, amount: 20 },
		{ id: 3, amount: 30 }
	];

	it('sums all rows (SUM(*))', () => {
		const source = new Memory({ initialData, pk: 'id', schema: null });
		const conn = source.connect();
		const max = new MaxOperator(conn, { column: 'amount' });
		const view = new View(max, defaultComparator('max'));

		expect(view.materialize()).toStrictEqual([{ max: 30 }]);
		source.add({ id: 4, amount: 40 });
		expect(view.currentState()).toStrictEqual([{ max: 40 }]);
		source.remove({ id: 1, amount: 10 });
		expect(view.currentState()).toStrictEqual([{ max: 40 }]);
		expect(view.materialize()).toStrictEqual([{ max: 40 }]);
		source.remove({ id: 4, amount: 40 });
		expect(view.currentState()).toStrictEqual([{ max: 30 }]);
		expect(view.materialize()).toStrictEqual([{ max: 30 }]);
		source.remove({ id: 3, amount: 30 });
		source.remove({ id: 2, amount: 20 });
		expect(view.currentState()).toStrictEqual([{ max: undefined }]);
	});

	it('sums all rows (SUM(*)) with subscribe', () => {
		const source = new Memory({ initialData, pk: 'id', schema: null });
		const conn = source.connect();
		const max = new MaxOperator(conn, { column: 'amount' });
		const view = new View(max, defaultComparator('max'));
		let result = view.materialize();
		view.subscribe((r) => (result = r));
		expect(result).toStrictEqual([{ max: 30 }]);
		source.add({ id: 4, amount: 40 });
		expect(result).toStrictEqual([{ max: 40 }]);
		source.remove({ id: 1, amount: 10 });
		expect(result).toStrictEqual([{ max: 40 }]);
		source.remove({ id: 4, amount: 40 });
		expect(result).toStrictEqual([{ max: 30 }]);
		source.remove({ id: 3, amount: 30 });
		source.remove({ id: 2, amount: 20 });
		expect(result).toStrictEqual([{ max: undefined }]);
	});
});
