import { describe, expect, it } from 'vitest';
import { BTree } from './tree.ts';
import { defaultComparator } from '../sources/memory.ts';

type User = { id: number; age: number };

describe('btree', () => {
	const numberComparator = (a: number, b: number): number => {
		if (a < b) return -1;
		if (a > b) return 1;
		return 0;
	};

	const userByIdComparator = (a: User, b: User): number => {
		return a.id - b.id;
	};

	const userByAgeComparator = (a: User, b: User): number => {
		return a.age - b.age;
	};

	it('inserts and retrieves values', () => {
		const btree = new BTree<number>(numberComparator);

		btree.add(10);
		btree.add(20);
		btree.add(5);
		btree.add(15);
		btree.add(30);
		btree.add(4);

		expect(btree.get(4)).toBe(4);
		expect(btree.get(10)).toBe(10);
		expect(btree.has(15)).toBe(true);
		expect(btree.has(100)).toBe(false);
	});

	it('deletes values', () => {
		const btree = new BTree<number>(numberComparator);

		btree.add(10);
		btree.add(20);
		btree.add(5);

		expect(btree.delete(10)).toBe(true);
		expect(btree.has(10)).toBe(false);
		expect(btree.delete(100)).toBe(false);
	});

	it('inserts and retrieves values - user objects', () => {
		const btree = new BTree<User>(userByIdComparator);

		const sammy = { id: 4, age: 30 };
		const emily = { id: 3, age: 25 };
		const fern = { id: 2, age: 28 };
		const del = { id: 1, age: 35 };

		btree.add(sammy);
		btree.add(emily);
		btree.add(fern);
		btree.add(del);

		expect(btree.get({ id: 4, age: 0 })).toStrictEqual(sammy);
		expect(btree.get({ id: 3, age: 0 })).toStrictEqual(emily);
		expect(btree.has({ id: 1, age: 0 })).toBe(true);
		expect(btree.has({ id: 100, age: 0 })).toBe(false);
	});

	it('iterates over values', () => {
		const btree = new BTree<number>(numberComparator);

		btree.add(10);
		btree.add(5);
		btree.add(20);
		btree.add(15);

		const values = Array.from(btree.values());
		expect(values).toEqual([5, 10, 15, 20]);
	});

	it('iterates from a specific value', () => {
		const btree = new BTree<number>(numberComparator);

		btree.add(10);
		btree.add(5);
		btree.add(20);
		btree.add(15);
		btree.add(25);

		const values = Array.from(btree.valuesFrom(15));
		expect(values).toEqual([15, 20, 25]);
	});

	it('iterates in reverse', () => {
		const btree = new BTree<number>(numberComparator);

		btree.add(10);
		btree.add(5);
		btree.add(20);
		btree.add(15);

		const values = Array.from(btree.valuesReversed());
		expect(values).toEqual([20, 15, 10, 5]);
	});

	describe('range queries with user objects', () => {
		const btree = new BTree<User>(userByAgeComparator);

		// Add some users
		Array.from({ length: 100 }).forEach((_, i) => {
			btree.add({ id: i, age: i });
		});

		it('gets users in age range', () => {
			const usersInRange: User[] = [];
			for (const user of btree.valuesFrom({ id: 0, age: 30 }, true)) {
				if (user.age > 35) break;
				usersInRange.push(user);
			}

			expect(usersInRange.length).toBe(6);
			expect(usersInRange[0].age).toBe(30);
			expect(usersInRange[5].age).toBe(35);
		});
	});

	describe('bug with get', () => {
		const pk = 'id';
		const comparator = defaultComparator(pk);
		const tree = new BTree(comparator);
		const name20 = {
			id: 20,
			name: 'name-20',
			age: 99
		};
		const initialData = [
			{
				id: 11,
				name: 'name-11',
				age: 35
			},
			{
				id: 12,
				name: 'name-12',
				age: 20
			},
			{
				id: 13,
				name: 'name-13',
				age: 13
			},
			{
				id: 14,
				name: 'name-14',
				age: 35
			},
			{
				id: 15,
				name: 'name-15',
				age: 89
			},
			{
				id: 16,
				name: 'name-16',
				age: 87
			},
			{
				id: 17,
				name: 'name-17',
				age: 51
			},
			{
				id: 18,
				name: 'name-18',
				age: 66
			},
			{
				id: 19,
				name: 'name-19',
				age: 89
			},
			name20,
			{
				id: 21,
				name: 'name-21',
				age: 55
			},
			{
				id: 22,
				name: 'name-22',
				age: 69
			},
			{
				id: 23,
				name: 'name-23',
				age: 77
			},
			{
				id: 24,
				name: 'name-24',
				age: 69
			},
			{
				id: 25,
				name: 'name-25',
				age: 56
			},
			{
				id: 26,
				name: 'name-26',
				age: 48
			},
			{
				id: 27,
				name: 'name-27',
				age: 71
			}
		];
		initialData.forEach((row) => tree.add(row));

		it('gets barf', () => {
			expect(tree.get(name20)).toStrictEqual(name20);
			// expect(tree.delete(name20)).toStrictEqual(true);
			// expect(tree.get(name20)).toBeUndefined();
		});
	});
});
