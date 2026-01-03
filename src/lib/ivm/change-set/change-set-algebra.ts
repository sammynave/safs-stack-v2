import { ChangeSet } from './change-set.ts';

export class ChangeSetAlgebra {
	static zero<T>(): ChangeSet<T> {
		return new ChangeSet([]);
	}

	static add<T>(a: ChangeSet<T>, b: ChangeSet<T>): ChangeSet<T> {
		if (a.isEmpty()) return b;
		if (b.isEmpty()) return a;

		return a.concat(b).mergeRecords();
	}

	static subtract<T>(a: ChangeSet<T>, b: ChangeSet<T>): ChangeSet<T> {
		return this.add(a, this.negate(b));
	}

	static negate<T>(a: ChangeSet<T>): ChangeSet<T> {
		return new ChangeSet(a.data.map(([r, w]) => [r, -w]));
	}
}
