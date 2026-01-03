import { describe, bench } from 'vitest';
import { BTree } from './tree.ts';

type User = { id: number; age: number };

describe('btree', () => {
	const ageComparator = (a: User, b: User): number => a.age - b.age;

	const btree = new BTree<User>(ageComparator);
	const m = new Map<number, User>();
	const a = [];

	Array.from({ length: 100000 }).forEach((_, i) => {
		const user = { id: i, age: i };
		m.set(i, user);
		btree.add(user);
		a.push(user);
	});

	bench('btree', () => {
		const btreeResults: User[] = [];
		for (const user of btree.valuesFrom({ id: 0, age: 30000 }, true)) {
			if (user.age > 30003) break;
			btreeResults.push(user);
		}
	});

	bench('map', () => {
		Array.from(m.values())
			.filter((u) => u.age >= 30000 && u.age <= 30003)
			.sort((a, b) => a.age - b.age);
	});

	bench('array', () => {
		a.filter((u) => u.age >= 30000 && u.age <= 30003).sort((a, b) => a.age - b.age);
	});
});
