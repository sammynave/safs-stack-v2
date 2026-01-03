import { describe, expect, it } from 'vitest';
import { defaultComparator, Memory } from '../../sources/memory.ts';
import { View } from '../../sinks/view.ts';
import { MinOperator } from './min-opertator.ts';

describe('MinOperator', () => {
	type Row = { id: number; amount: number };
	const initialData: Row[] = [
		{ id: 1, amount: 10 },
		{ id: 2, amount: 20 },
		{ id: 3, amount: 30 }
	];

	it('sums all rows (SUM(*))', () => {
		const source = new Memory({ initialData, pk: 'id', schema: null });
		const conn = source.connect();
		const min = new MinOperator(conn, { column: 'amount' });
		const view = new View(min, defaultComparator('min'));

		expect(view.materialize()).toStrictEqual([{ min: 10 }]);
		source.add({ id: 4, amount: 5 });
		expect(view.currentState()).toStrictEqual([{ min: 5 }]);
		source.remove({ id: 4, amount: 5 });
		expect(view.currentState()).toStrictEqual([{ min: 10 }]);
		expect(view.materialize()).toStrictEqual([{ min: 10 }]);
		source.remove({ id: 3, amount: 30 });
		source.remove({ id: 2, amount: 20 });
		source.remove({ id: 1, amount: 10 });
		expect(view.currentState()).toStrictEqual([{ min: undefined }]);
	});

	it('sums all rows (SUM(*)) with subscribe', () => {
		const source = new Memory({ initialData, pk: 'id', schema: null });
		const conn = source.connect();
		const min = new MinOperator(conn, { column: 'amount' });
		const view = new View(min, defaultComparator('min'));
		let result = view.materialize();
		view.subscribe((r) => (result = r));

		expect(result).toStrictEqual([{ min: 10 }]);
		source.add({ id: 4, amount: 5 });
		expect(result).toStrictEqual([{ min: 5 }]);
		source.remove({ id: 4, amount: 5 });
		expect(result).toStrictEqual([{ min: 10 }]);
		expect(result).toStrictEqual([{ min: 10 }]);
		source.remove({ id: 3, amount: 30 });
		source.remove({ id: 2, amount: 20 });
		source.remove({ id: 1, amount: 10 });
		expect(result).toStrictEqual([{ min: undefined }]);
	});
});
