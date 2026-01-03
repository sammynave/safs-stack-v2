import { describe, expect, it } from 'vitest';
import { Memory } from '../sources/memory.ts';
import { LimitOperator } from './limit-operator.ts';
import { LogSink } from './log-operator.ts';
import { View } from '../sinks/view.ts';

describe('top-k', () => {
	it('works', () => {
		// Setup: comparator that keeps items with lowest value
		type Row = { id: number; value: number };
		const comparator = (a: unknown, b: unknown): 0 | 1 | -1 => {
			const rowA = a as Row;
			const rowB = b as Row;
			if (rowA.value < rowB.value) return -1;
			if (rowA.value > rowB.value) return 1;
			if (rowA.id < rowB.id) return -1;
			if (rowA.id > rowB.id) return 1;
			return 0;
		};

		// Create a source with backup rows for refilling
		const backupRows: Row[] = [
			{ id: 6, value: 60 },
			{ id: 7, value: 35 },
			{ id: 8, value: 80 }
		];
		const source = new Memory({ initialData: backupRows, pk: 'id', schema: null });
		const conn = source.connect(['value', 'asc'], comparator);
		// Initialize with limit of 3
		const limit = new LimitOperator(conn, 3, comparator);
		const view = new View(limit, comparator);
		// Test 1: Add 5 items, should keep top 3 (lowest values)
		const moreRows = [
			{ id: 1, value: 10 },
			{ id: 2, value: 50 },
			{ id: 3, value: 20 },
			{ id: 4, value: 40 },
			{ id: 5, value: 30 }
		];

		const top3AscValue = [
			{ id: 1, value: 10 },
			{ id: 3, value: 20 },
			{ id: 5, value: 30 }
		];
		expect(limit.currentState.data, 'it is lazy').toStrictEqual([]);
		expect(view.materialize(), 'log should have initial data').toStrictEqual([
			{ id: 7, value: 35 },
			{ id: 6, value: 60 },
			{ id: 8, value: 80 }
		]);
		for (const row of moreRows) {
			source.add(row);
		}
		expect(view.materialize(), 'log should have top 3').toStrictEqual(top3AscValue);

		const newTop3AscValue = [
			[{ id: 1, value: 10 }, 1],
			[{ id: 5, value: 30 }, 1],
			[{ id: 7, value: 35 }, 1]
		];
		source.remove({ id: 3, value: 20 });
		const afterRemoveData = Array.from(limit.pull());
		expect(afterRemoveData, 'after data has been removed').toStrictEqual(newTop3AscValue);

		const newNewTop3AscValue = [
			[{ id: 1, value: 10 }, 1],
			[{ id: 9, value: 15 }, 1],
			[{ id: 5, value: 30 }, 1]
		];
		// Test 3: Add item with value 15 (better than current worst of 35)
		source.add({ id: 9, value: 15 });
		const afterAddData = Array.from(limit.pull());
		expect(afterAddData, 'new data added').toStrictEqual(newNewTop3AscValue);
	});
	it('works with view as sink', () => {
		// Setup: comparator that keeps items with lowest value
		type Row = { id: number; value: number };
		const comparator = (a: unknown, b: unknown): 0 | 1 | -1 => {
			const rowA = a as Row;
			const rowB = b as Row;
			if (rowA.value < rowB.value) return -1;
			if (rowA.value > rowB.value) return 1;
			if (rowA.id < rowB.id) return -1;
			if (rowA.id > rowB.id) return 1;
			return 0;
		};

		// Create a source with backup rows for refilling
		const backupRows: Row[] = [
			{ id: 6, value: 60 },
			{ id: 7, value: 35 },
			{ id: 8, value: 80 }
		];
		const source = new Memory({ initialData: backupRows, pk: 'id', schema: null });

		const conn = source.connect(['value', 'asc'], comparator);
		// Initialize with limit of 3
		const limit = new LimitOperator(conn, 3, comparator);
		const view = new View(limit, comparator);
		// Test 1: Add 5 items, should keep top 3 (lowest values)
		const moreRows = [
			{ id: 1, value: 10 },
			{ id: 2, value: 50 },
			{ id: 3, value: 20 },
			{ id: 4, value: 40 },
			{ id: 5, value: 30 }
		];

		const top3AscValue = [
			{ id: 1, value: 10 },
			{ id: 3, value: 20 },
			{ id: 5, value: 30 }
		];

		expect(view.materialize(), 'view should have no data').toStrictEqual([
			{ id: 7, value: 35 },
			{ id: 6, value: 60 },
			{ id: 8, value: 80 }
		]);
		for (const row of moreRows) {
			source.add(row);
		}
		expect(view.materialize(), 'log should have top 3').toStrictEqual(top3AscValue);

		const newTop3AscValue = [
			{ id: 1, value: 10 },
			{ id: 5, value: 30 },
			{ id: 7, value: 35 }
		];
		source.remove({ id: 3, value: 20 });
		expect(view.materialize(), 'after data has been removed').toStrictEqual(newTop3AscValue);

		const newNewTop3AscValue = [
			{ id: 1, value: 10 },
			{ id: 9, value: 15 },
			{ id: 5, value: 30 }
		];
		// Test 3: Add item with value 15 (better than current worst of 35)
		source.add({ id: 9, value: 15 });

		expect(view.materialize(), 'new data added').toStrictEqual(newNewTop3AscValue);
	});
	it('works with view as sink and subscribe', () => {
		// Setup: comparator that keeps items with lowest value
		type Row = { id: number; value: number };
		const comparator = (a: unknown, b: unknown): 0 | 1 | -1 => {
			const rowA = a as Row;
			const rowB = b as Row;
			if (rowA.value < rowB.value) return -1;
			if (rowA.value > rowB.value) return 1;
			if (rowA.id < rowB.id) return -1;
			if (rowA.id > rowB.id) return 1;
			return 0;
		};

		// Create a source with backup rows for refilling
		const backupRows: Row[] = [
			{ id: 6, value: 60 },
			{ id: 7, value: 35 },
			{ id: 8, value: 80 }
		];
		const source = new Memory({ initialData: backupRows, pk: 'id', schema: null });

		const conn = source.connect(['value', 'asc'], comparator);
		// Initialize with limit of 3
		const limit = new LimitOperator(conn, 3, comparator);
		const view = new View(limit, comparator);
		// Test 1: Add 5 items, should keep top 3 (lowest values)
		const moreRows = [
			{ id: 1, value: 10 },
			{ id: 2, value: 50 },
			{ id: 3, value: 20 },
			{ id: 4, value: 40 },
			{ id: 5, value: 30 }
		];

		const top3AscValue = [
			{ id: 1, value: 10 },
			{ id: 3, value: 20 },
			{ id: 5, value: 30 }
		];
		let results = view.materialize();
		view.subscribe((r) => {
			results = r;
		});
		expect(results, 'view should have initial data').toStrictEqual([
			{ id: 7, value: 35 },
			{ id: 6, value: 60 },
			{ id: 8, value: 80 }
		]);
		for (const row of moreRows) {
			source.add(row);
		}
		expect(results, 'log should have top 3').toStrictEqual(top3AscValue);

		const newTop3AscValue = [
			{ id: 1, value: 10 },
			{ id: 5, value: 30 },
			{ id: 7, value: 35 }
		];
		source.remove({ id: 3, value: 20 });
		expect(results, 'after data has been removed').toStrictEqual(newTop3AscValue);

		const newNewTop3AscValue = [
			{ id: 1, value: 10 },
			{ id: 9, value: 15 },
			{ id: 5, value: 30 }
		];
		// Test 3: Add item with value 15 (better than current worst of 35)
		source.add({ id: 9, value: 15 });

		expect(results, 'new data added').toStrictEqual(newNewTop3AscValue);

		source.add({ id: 10, value: 12 });
		source.add({ id: 11, value: 11 });
		source.add({ id: 12, value: 13 });

		expect(results, 'more data added').toStrictEqual([
			{ id: 1, value: 10 },
			{ id: 11, value: 11 },
			{ id: 10, value: 12 }
		]);
		source.remove({ id: 10, value: 12 });
		expect(results, 'more data added').toStrictEqual([
			{ id: 1, value: 10 },
			{ id: 11, value: 11 },
			{ id: 12, value: 13 }
		]);
	});
});
