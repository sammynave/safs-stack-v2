import { ChangeSet } from './change-set.ts';

export class ChangeSetOperators {
	static distinct<T>(changeSet: ChangeSet<T>): ChangeSet<T> {
		const map = changeSet.mergeRecords().data.reduce((acc, [r, w]) => {
			if (w < 1) return acc;
			if (acc.has(r)) return acc;
			acc.set(r, 1);
			return acc;
		}, new Map());
		return new ChangeSet(Array.from(map.entries()));
	}
}
