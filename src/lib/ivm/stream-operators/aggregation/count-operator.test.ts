import { describe, expect, it } from 'vitest';
import { defaultComparator, Memory } from '../../sources/memory.ts';
import { CountOperator } from './count-operator.ts';
import { View } from '../../sinks/view.ts';

describe('CountOperator', () => {
	it('counts all rows (COUNT(*))', () => {
		type Row = { id: number; amount: number };
		const initialData: Row[] = [
			{ id: 1, amount: 10 },
			{ id: 2, amount: 20 },
			{ id: 3, amount: 30 }
		];

		const source = new Memory({ initialData, pk: 'id', schema: null });
		const conn = source.connect();
		const count = new CountOperator(conn);
		const view = new View(count, defaultComparator('count'));

		// Initial count
		expect(view.materialize()).toStrictEqual([{ count: 3 }]);

		// Add a row
		source.add({ id: 4, amount: 40 });
		expect(view.materialize()).toStrictEqual([{ count: 4 }]);

		// Remove a row
		source.remove({ id: 2, amount: 20 });
		expect(view.materialize()).toStrictEqual([{ count: 3 }]);

		// Add multiple rows
		source.add({ id: 5, amount: 50 });
		source.add({ id: 6, amount: 60 });
		expect(view.materialize()).toStrictEqual([{ count: 5 }]);
	});

	it('counts all rows (COUNT(*)) with subscribe', () => {
		type Row = { id: number; amount: number };
		const initialData: Row[] = [
			{ id: 1, amount: 10 },
			{ id: 2, amount: 20 },
			{ id: 3, amount: 30 }
		];

		const source = new Memory({ initialData, pk: 'id', schema: null });
		const conn = source.connect();
		const count = new CountOperator(conn);
		const view = new View(count, defaultComparator('count'));

		let result = view.materialize();
		view.subscribe((r) => (result = r));
		// Initial count
		expect(result).toStrictEqual([{ count: 3 }]);

		// Add a row
		source.add({ id: 4, amount: 40 });
		expect(result).toStrictEqual([{ count: 4 }]);

		// Remove a row
		source.remove({ id: 2, amount: 20 });
		expect(result).toStrictEqual([{ count: 3 }]);

		// Add multiple rows
		source.add({ id: 5, amount: 50 });
		source.add({ id: 6, amount: 60 });
		expect(result).toStrictEqual([{ count: 5 }]);
	});
	it('counts non-null values (COUNT(column))', () => {
		type Row = { id: number; amount: number | null };
		const initialData: Row[] = [
			{ id: 1, amount: 10 },
			{ id: 2, amount: null },
			{ id: 3, amount: 30 },
			{ id: 4, amount: null }
		];

		const source = new Memory({ initialData, pk: 'id', schema: null });
		const conn = source.connect();
		const count = new CountOperator(conn, { column: 'amount' });
		const view = new View(count, defaultComparator('count'));

		// Initial count (only non-null amounts)
		expect(view.materialize()).toStrictEqual([{ count: 2 }]);

		// Add row with non-null amount
		source.add({ id: 5, amount: 50 });
		expect(view.materialize()).toStrictEqual([{ count: 3 }]);

		// Add row with null amount (shouldn't change count)
		source.add({ id: 6, amount: null });
		expect(view.materialize()).toStrictEqual([{ count: 3 }]);

		// Remove row with non-null amount
		source.remove({ id: 1, amount: 10 });
		expect(view.materialize()).toStrictEqual([{ count: 2 }]);

		// Remove row with null amount (shouldn't change count)
		source.remove({ id: 2, amount: null });
		expect(view.materialize()).toStrictEqual([{ count: 2 }]);

		let subCount;
		view.subscribe((c) => {
			subCount = c;
		});

		expect(subCount).toStrictEqual([{ count: 2 }]);

		source.add({ id: 10, amount: null });
		expect(subCount).toStrictEqual([{ count: 2 }]);
		source.add({ id: 11, amount: 50 });
		expect(subCount).toStrictEqual([{ count: 3 }]);
	});
});
