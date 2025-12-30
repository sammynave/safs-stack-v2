import { describe, expect, it } from 'vitest';
import { Memory } from '../sources/memory.ts';
import { GroupByOperator } from './group-by-operator.ts';
import { View } from '../sinks/view.ts';

describe('group by', () => {
	it('works', () => {
		const initialData = [
			{ userId: 1, id: 1, amount: 10 },
			{ userId: 1, id: 2, amount: 20 },
			{ userId: 2, id: 3, amount: 30 },
			{ userId: 3, id: 4, amount: 40 }
		];

		const source = new Memory({ initialData, pk: 'id', schema: {} });
		const sourceComparator = (rowA, rowB) => {
			if (rowA.userId < rowB.userId) return -1;
			if (rowA.userId > rowB.userId) return 1;
			if (rowA.id < rowB.id) return -1;
			if (rowA.id > rowB.id) return 1;
			return 0;
		};
		const conn = source.connect(['id', 'asc'], sourceComparator);
		const groupBy = new GroupByOperator(conn, ['userId'], (rowA, rowB) => {
			// Compare by id to ensure uniqueness within groups
			if (rowA.id < rowB.id) return -1;
			if (rowA.id > rowB.id) return 1;
			return 0;
		});
		const view = new View(groupBy, (a, b) => a.keyValues.userId - b.keyValues.userId);

		const results = view.materialize();
		expect(results, 'first pull is right').toStrictEqual([
			{
				keys: ['userId'],
				keyValues: { userId: 1 },
				rows: [
					{ userId: 1, id: 1, amount: 10 },
					{ userId: 1, id: 2, amount: 20 }
				]
			},
			{ keys: ['userId'], keyValues: { userId: 2 }, rows: [{ userId: 2, id: 3, amount: 30 }] },
			{ keys: ['userId'], keyValues: { userId: 3 }, rows: [{ userId: 3, id: 4, amount: 40 }] }
		]);

		source.add({ userId: 2, id: 5, amount: 50 });
		expect(view.materialize(), 'second pull').toStrictEqual([
			{
				keys: ['userId'],
				keyValues: { userId: 1 },
				rows: [
					{ userId: 1, id: 1, amount: 10 },
					{ userId: 1, id: 2, amount: 20 }
				]
			},
			{
				keys: ['userId'],
				keyValues: { userId: 2 },
				rows: [
					{ userId: 2, id: 3, amount: 30 },
					{ userId: 2, id: 5, amount: 50 }
				]
			},
			{ keys: ['userId'], keyValues: { userId: 3 }, rows: [{ userId: 3, id: 4, amount: 40 }] }
		]);
	});
});
