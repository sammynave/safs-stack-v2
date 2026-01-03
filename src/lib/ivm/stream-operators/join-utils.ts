import type { BTree, Comparator } from '../b-plus-tree/tree.ts';

export type IndexedRow<R> = {
	key: unknown; // The join key
	row: R; // The actual row data
};

export function compareStrings(a: string, b: string): number {
	if (a < b) return -1;
	if (a > b) return 1;
	return 0;
}

export function compareKeys(a: unknown, b: unknown): number {
	if (typeof a === 'number' && typeof b === 'number') {
		return a - b;
	}
	if (typeof a === 'string' && typeof b === 'string') {
		return compareStrings(a, b);
	}
	const jsonA = JSON.stringify(a);
	const jsonB = JSON.stringify(b);
	return compareStrings(jsonA, jsonB);
}

export function createIndexedComparator<R>(): Comparator<IndexedRow<R>> {
	return (a: IndexedRow<R>, b: IndexedRow<R>) => {
		// Primary comparison by join key
		const keyCompare = compareKeys(a.key, b.key);
		if (keyCompare !== 0) return keyCompare;

		// Secondary comparison by full row (to distinguish rows with same key)
		const jsonA = JSON.stringify(a.row);
		const jsonB = JSON.stringify(b.row);
		return compareStrings(jsonA, jsonB);
	};
}

export function* findMatchingRows<R>(
	storage: BTree<IndexedRow<R>>,
	joinKey: unknown
): Generator<R> {
	// Create a probe to position the iterator at the first matching key
	const probe: IndexedRow<R> = { key: joinKey, row: null as unknown as R };

	// Use valuesFrom to efficiently jump to the target key range
	for (const indexed of storage.valuesFrom(probe, true)) {
		if (compareKeys(indexed.key, joinKey) === 0) {
			yield indexed.row;
		} else {
			// Once we pass the target key, stop (tree is sorted)
			break;
		}
	}
}
