import { describe, expect, it } from 'vitest';
import { Memory } from '../sources/memory.ts';
import { MultiRowCombineOperator } from './multi-row-combine-operator.ts';
import { View } from '../sinks/view.ts';
import { ChangeSet } from '../change-set/change-set.ts';

describe('MultiRowCombineOperator', () => {
	it('merges rows with matching keys', () => {
		// Setup: Two sources with GROUP BY results
		const leftData = [
			{ userId: 1, count: 5 },
			{ userId: 2, count: 3 }
		];
		const rightData = [
			{ userId: 1, sum: 100 },
			{ userId: 2, sum: 60 }
		];

		const leftSource = new Memory({ initialData: leftData, pk: 'userId', schema: {} });
		const rightSource = new Memory({ initialData: rightData, pk: 'userId', schema: {} });

		const leftConn = leftSource.connect(['userId', 'asc'], (a, b) => a.userId - b.userId);
		const rightConn = rightSource.connect(['userId', 'asc'], (a, b) => a.userId - b.userId);

		const combineOp = new MultiRowCombineOperator(leftConn, rightConn, (left, right) => ({
			userId: left.userId,
			count: left.count,
			sum: right.sum
		}));

		const view = new View(combineOp, (a, b) => a.userId - b.userId);

		expect(view.materialize()).toStrictEqual([
			{ userId: 1, count: 5, sum: 100 },
			{ userId: 2, count: 3, sum: 60 }
		]);
	});

	it('only produces results when both sides have matching keys', () => {
		// Left has keys [1, 2], Right has keys [2, 3]
		// Only key 2 should produce a result
		const leftData = [
			{ userId: 1, count: 5 },
			{ userId: 2, count: 3 }
		];
		const rightData = [
			{ userId: 2, sum: 60 },
			{ userId: 3, sum: 90 }
		];

		const leftSource = new Memory({ initialData: leftData, pk: 'userId', schema: {} });
		const rightSource = new Memory({ initialData: rightData, pk: 'userId', schema: {} });

		const leftConn = leftSource.connect(['userId', 'asc'], (a, b) => a.userId - b.userId);
		const rightConn = rightSource.connect(['userId', 'asc'], (a, b) => a.userId - b.userId);

		const combineOp = new MultiRowCombineOperator(leftConn, rightConn, (left, right) => ({
			userId: left.userId,
			count: left.count,
			sum: right.sum
		}));

		const view = new View(combineOp, (a, b) => a.userId - b.userId);

		expect(view.materialize()).toStrictEqual([{ userId: 2, count: 3, sum: 60 }]);
	});

	it('removes result when left row is deleted', () => {
		const leftData = [
			{ userId: 1, count: 5 },
			{ userId: 2, count: 3 }
		];
		const rightData = [
			{ userId: 1, sum: 100 },
			{ userId: 2, sum: 60 }
		];

		const leftSource = new Memory({ initialData: leftData, pk: 'userId', schema: {} });
		const rightSource = new Memory({ initialData: rightData, pk: 'userId', schema: {} });

		const leftConn = leftSource.connect(['userId', 'asc'], (a, b) => a.userId - b.userId);
		const rightConn = rightSource.connect(['userId', 'asc'], (a, b) => a.userId - b.userId);

		const combineOp = new MultiRowCombineOperator(leftConn, rightConn, (left, right) => ({
			userId: left.userId,
			count: left.count,
			sum: right.sum
		}));

		const view = new View(combineOp, (a, b) => a.userId - b.userId);

		expect(view.materialize()).toStrictEqual([
			{ userId: 1, count: 5, sum: 100 },
			{ userId: 2, count: 3, sum: 60 }
		]);

		// Delete userId 1 from left
		leftSource.remove({ userId: 1, count: 5 });

		expect(view.materialize()).toStrictEqual([{ userId: 2, count: 3, sum: 60 }]);
	});

	it('removes result when right row is deleted', () => {
		const leftData = [
			{ userId: 1, count: 5 },
			{ userId: 2, count: 3 }
		];
		const rightData = [
			{ userId: 1, sum: 100 },
			{ userId: 2, sum: 60 }
		];

		const leftSource = new Memory({ initialData: leftData, pk: 'userId', schema: {} });
		const rightSource = new Memory({ initialData: rightData, pk: 'userId', schema: {} });

		const leftConn = leftSource.connect(['userId', 'asc'], (a, b) => a.userId - b.userId);
		const rightConn = rightSource.connect(['userId', 'asc'], (a, b) => a.userId - b.userId);

		const combineOp = new MultiRowCombineOperator(leftConn, rightConn, (left, right) => ({
			userId: left.userId,
			count: left.count,
			sum: right.sum
		}));

		const view = new View(combineOp, (a, b) => a.userId - b.userId);

		expect(view.materialize()).toStrictEqual([
			{ userId: 1, count: 5, sum: 100 },
			{ userId: 2, count: 3, sum: 60 }
		]);

		// Delete userId 1 from right
		rightSource.remove({ userId: 1, sum: 100 });

		expect(view.materialize()).toStrictEqual([{ userId: 2, count: 3, sum: 60 }]);
	});

	it('adds result when matching row is added', () => {
		// Start with left having userId 1, right having userId 2
		const leftData = [{ userId: 1, count: 5 }];
		const rightData = [{ userId: 2, sum: 60 }];

		const leftSource = new Memory({ initialData: leftData, pk: 'userId', schema: {} });
		const rightSource = new Memory({ initialData: rightData, pk: 'userId', schema: {} });

		const leftConn = leftSource.connect(['userId', 'asc'], (a, b) => a.userId - b.userId);
		const rightConn = rightSource.connect(['userId', 'asc'], (a, b) => a.userId - b.userId);

		const combineOp = new MultiRowCombineOperator(leftConn, rightConn, (left, right) => ({
			userId: left.userId,
			count: left.count,
			sum: right.sum
		}));

		const view = new View(combineOp, (a, b) => a.userId - b.userId);

		// No matching keys initially
		expect(view.materialize()).toStrictEqual([]);

		// Add matching row to right
		rightSource.add({ userId: 1, sum: 100 });

		expect(view.materialize()).toStrictEqual([{ userId: 1, count: 5, sum: 100 }]);

		// Add matching row to left
		leftSource.add({ userId: 2, count: 3 });

		expect(view.materialize()).toStrictEqual([
			{ userId: 1, count: 5, sum: 100 },
			{ userId: 2, count: 3, sum: 60 }
		]);
	});

	it('updates result when value changes', () => {
		const leftData = [{ userId: 1, count: 5 }];
		const rightData = [{ userId: 1, sum: 100 }];

		const leftSource = new Memory({ initialData: leftData, pk: 'userId', schema: {} });
		const rightSource = new Memory({ initialData: rightData, pk: 'userId', schema: {} });

		const leftConn = leftSource.connect(['userId', 'asc'], (a, b) => a.userId - b.userId);
		const rightConn = rightSource.connect(['userId', 'asc'], (a, b) => a.userId - b.userId);

		const combineOp = new MultiRowCombineOperator(leftConn, rightConn, (left, right) => ({
			userId: left.userId,
			count: left.count,
			sum: right.sum
		}));

		const view = new View(combineOp, (a, b) => a.userId - b.userId);

		expect(view.materialize()).toStrictEqual([{ userId: 1, count: 5, sum: 100 }]);

		// Update left count
		leftSource.update({ userId: 1, count: 5 }, { userId: 1, count: 10 });

		expect(view.materialize()).toStrictEqual([{ userId: 1, count: 10, sum: 100 }]);

		// Update right sum
		rightSource.update({ userId: 1, sum: 100 }, { userId: 1, sum: 200 });

		expect(view.materialize()).toStrictEqual([{ userId: 1, count: 10, sum: 200 }]);
	});

	it('handles empty sources', () => {
		const leftSource = new Memory({ initialData: [], pk: 'userId', schema: {} });
		const rightSource = new Memory({ initialData: [], pk: 'userId', schema: {} });

		const leftConn = leftSource.connect(['userId', 'asc'], (a, b) => a.userId - b.userId);
		const rightConn = rightSource.connect(['userId', 'asc'], (a, b) => a.userId - b.userId);

		const combineOp = new MultiRowCombineOperator(leftConn, rightConn, (left, right) => ({
			userId: left.userId,
			count: left.count,
			sum: right.sum
		}));

		const view = new View(combineOp, (a, b) => a.userId - b.userId);

		expect(view.materialize()).toStrictEqual([]);
	});

	it('does not emit changes when update produces same result', () => {
		const leftData = [{ userId: 1, count: 5 }];
		const rightData = [{ userId: 1, sum: 100 }];

		const leftSource = new Memory({ initialData: leftData, pk: 'userId', schema: {} });
		const rightSource = new Memory({ initialData: rightData, pk: 'userId', schema: {} });

		const leftConn = leftSource.connect(['userId', 'asc'], (a, b) => a.userId - b.userId);
		const rightConn = rightSource.connect(['userId', 'asc'], (a, b) => a.userId - b.userId);

		// Merger that ignores the actual values - always returns same result
		const combineOp = new MultiRowCombineOperator(leftConn, rightConn, (left, right) => ({
			userId: left.userId,
			constant: 42
		}));

		const view = new View(combineOp, (a, b) => a.userId - b.userId);

		expect(view.materialize()).toStrictEqual([{ userId: 1, constant: 42 }]);

		const initialSize = view.size;

		// Update left - but merger produces same result
		leftSource.update({ userId: 1, count: 5 }, { userId: 1, count: 999 });

		// Result should be unchanged
		expect(view.materialize()).toStrictEqual([{ userId: 1, constant: 42 }]);
		expect(view.size).toBe(initialSize);
	});

	it('handles updates to non-matching keys without spurious emissions', () => {
		const leftData = [{ userId: 1, count: 5 }];
		const rightData = [{ userId: 2, sum: 60 }];

		const leftSource = new Memory({ initialData: leftData, pk: 'userId', schema: {} });
		const rightSource = new Memory({ initialData: rightData, pk: 'userId', schema: {} });

		const leftConn = leftSource.connect(['userId', 'asc'], (a, b) => a.userId - b.userId);
		const rightConn = rightSource.connect(['userId', 'asc'], (a, b) => a.userId - b.userId);

		const combineOp = new MultiRowCombineOperator(leftConn, rightConn, (left, right) => ({
			userId: left.userId,
			count: left.count,
			sum: right.sum
		}));

		const view = new View(combineOp, (a, b) => a.userId - b.userId);

		// No matching keys
		expect(view.materialize()).toStrictEqual([]);

		// Update left (still no match)
		leftSource.update({ userId: 1, count: 5 }, { userId: 1, count: 10 });

		// Still no results
		expect(view.materialize()).toStrictEqual([]);

		// Update right (still no match)
		rightSource.update({ userId: 2, sum: 60 }, { userId: 2, sum: 120 });

		// Still no results
		expect(view.materialize()).toStrictEqual([]);
	});
});
