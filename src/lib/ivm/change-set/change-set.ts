/*
 * this is the z-set from the paper
 */

export class ChangeSet<T> {
	#data: Array<[T, number]> = [];
	#keyCache: Map<T, string> = new Map();
	#isMerged: boolean = false;

	constructor(data: Array<[T, number]>) {
		this.#data = data;
	}

	get data() {
		return this.#data;
	}

	append(d: [T, number]) {
		this.#data.push(d);
		this.#isMerged = false;
	}

	isMerged(): boolean {
		return this.#isMerged;
	}

	isSet() {
		return this.#data.every(([_, w]) => w === 1);
	}

	isPositive() {
		return this.#data.every(([_, w]) => w >= 0);
	}

	isEmpty() {
		return this.#data.length === 0;
	}

	mergeRecords(): ChangeSet<T> {
		if (this.#isMerged) {
			return this;
		}

		const mergedRecords = new Map<string, [T, number]>();

		for (const [record, weight] of this.#data) {
			const key = this.#getOrComputeKey(record);

			if (mergedRecords.has(key)) {
				const [existingRecord, existingWeight] = mergedRecords.get(key)!;
				mergedRecords.set(key, [existingRecord, existingWeight + weight]);
			} else {
				mergedRecords.set(key, [record, weight]);
			}
		}

		const result = new ChangeSet<T>([]);
		for (const [record, weight] of mergedRecords.values()) {
			if (weight !== 0) {
				result.append([record, weight]);
				const key = this.#getOrComputeKey(record);
				result.#keyCache.set(record, key);
			}
		}

		result.#isMerged = true;
		return result;
	}

	multiply(scalar: number) {
		return this.#data.reduce((acc, [r, w]) => {
			const newW = w * scalar;
			if (newW === 0) return acc;

			acc.append([r, newW]);
			return acc;
		}, new ChangeSet([]));
	}

	concat(other: ChangeSet<T>): ChangeSet<T> {
		const unioned = new ChangeSet<T>([]);
		for (const d of this.#data) {
			unioned.append(d);
		}
		for (const d of other.data) {
			unioned.append(d);
		}

		return unioned;
	}

	#getOrComputeKey(record: T): string {
		if (this.#keyCache.has(record)) {
			return this.#keyCache.get(record)!;
		}
		const key = JSON.stringify(record);
		this.#keyCache.set(record, key);
		return key;
	}
}
