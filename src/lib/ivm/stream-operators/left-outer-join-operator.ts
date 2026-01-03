import { assert } from '$lib/asserts.js';
import { BTree, type Comparator } from '../b-plus-tree/tree.ts';
import { ChangeSet } from '../change-set/change-set.ts';
import { NullSink } from '../sinks/null-sink.ts';
import type { Sink, Source } from './stream-operator-types.ts';
import { type IndexedRow, createIndexedComparator, findMatchingRows } from './join-utils.ts';

/**
 * LeftOuterJoinOperator - Performs a LEFT OUTER JOIN between two sources
 *
 * Returns all rows from the left source, with matching rows from the right source.
 * When no match exists on the right, returns [leftRow, null].
 *
 * Result type: [T, U | null]
 *
 * @example
 * ```typescript
 * const leftOuter = new LeftOuterJoinOperator(
 *   usersSource,
 *   ordersSource,
 *   (user) => user.userId,
 *   (order) => order.userId,
 *   resultComparator
 * );
 * // Returns all users, with their orders (or null if no orders)
 * ```
 */
/* stateful */
export class LeftOuterJoinOperator<T, U> implements Sink<[T, U | null]>, Source<[T, U | null]> {
	readonly #leftSource: Source<T>;
	readonly #rightSource: Source<U>;
	readonly #leftKeyExtractor: (row: T) => unknown;
	readonly #rightKeyExtractor: (row: U) => unknown;
	#sink: Sink<[T, U | null]> | typeof NullSink = NullSink;
	#results: BTree<[T, U | null]>;
	#leftStorage: BTree<IndexedRow<T>>;
	#rightStorage: BTree<IndexedRow<U>>;
	#leftMatchCounts: Map<string, number>;

	constructor(
		leftSource: Source<T>,
		rightSource: Source<U>,
		leftKeyExtractor: (row: T) => unknown,
		rightKeyExtractor: (row: U) => unknown,
		resultComparator: Comparator<[T, U | null]>
	) {
		this.#leftSource = leftSource;
		this.#rightSource = rightSource;
		this.#leftKeyExtractor = leftKeyExtractor;
		this.#rightKeyExtractor = rightKeyExtractor;
		this.#leftMatchCounts = new Map();

		this.#leftStorage = new BTree<IndexedRow<T>>(createIndexedComparator<T>());
		this.#rightStorage = new BTree<IndexedRow<U>>(createIndexedComparator<U>());
		this.#results = new BTree<[T, U | null]>(resultComparator);

		this.#processInitialData();

		this.#leftSource.setSink({
			push: (changeSet: ChangeSet<T>) => this.pushLeft(changeSet)
		});
		this.#rightSource.setSink({
			push: (changeSet: ChangeSet<U>) => this.pushRight(changeSet)
		});
	}

	get size() {
		return this.#results.size;
	}

	setSink(sink: Sink<[T, U | null]>) {
		assert(
			this.#sink === NullSink,
			Error(`Sink already set! Only 1 sink allowed`, { cause: this.#sink })
		);
		this.#sink = sink;
	}

	push() {
		throw Error('Can not push to LeftOuterJoinOperator directly. Use pushLeft or pushRight');
	}

	pushLeft(changeSet: ChangeSet<T>) {
		this.#push(changeSet, 'left');
	}

	pushRight(changeSet: ChangeSet<U>) {
		this.#push(changeSet, 'right');
	}

	*pull() {
		for (const joinedRow of this.#results.values()) {
			yield [joinedRow, 1] as [[T, U | null], number];
		}
	}

	disconnect() {
		this.#leftSource.disconnect(this);
		this.#rightSource.disconnect(this);
	}

	#updateStorage<R>(storage: BTree<IndexedRow<R>>, item: IndexedRow<R>, weight: number) {
		if (weight > 0) {
			storage.add(item);
		} else if (weight < 0) {
			storage.delete(item);
		} else {
			throw Error('Weight is 0');
		}
	}

	#getMatchCount(leftRowKey: string): number {
		return this.#leftMatchCounts.get(leftRowKey) || 0;
	}

	#setMatchCount(leftRowKey: string, count: number) {
		this.#leftMatchCounts.set(leftRowKey, count);
	}

	#deleteMatchCount(leftRowKey: string) {
		this.#leftMatchCounts.delete(leftRowKey);
	}

	#emitPair(pair: [T, U | null], weight: number, outputChangeSet: ChangeSet<[T, U | null]>) {
		outputChangeSet.append([pair, weight]);
		if (weight > 0) {
			this.#results.add(pair);
		} else {
			this.#results.delete(pair);
		}
	}

	#pushRow(options: {
		row: T | U;
		weight: number;
		joinKey: unknown;
		outputChangeSet: ChangeSet<[T, U | null]>;
		source: 'left' | 'right';
	}) {
		if (options.source === 'left') {
			const leftRow = options.row as T;
			const leftRowKey = JSON.stringify(leftRow);

			this.#updateStorage(
				this.#leftStorage,
				{ key: options.joinKey, row: leftRow },
				options.weight
			);

			if (options.weight > 0) {
				// LEFT ROW ADDED
				let matchCount = 0;
				for (const rightRow of findMatchingRows(this.#rightStorage, options.joinKey)) {
					this.#emitPair([leftRow, rightRow], 1, options.outputChangeSet);
					matchCount++;
				}

				if (matchCount === 0) {
					this.#emitPair([leftRow, null], 1, options.outputChangeSet);
				}
				this.#setMatchCount(leftRowKey, matchCount);
			} else {
				// LEFT ROW REMOVED
				const currentMatchCount = this.#getMatchCount(leftRowKey);
				if (currentMatchCount > 0) {
					for (const rightRow of findMatchingRows(this.#rightStorage, options.joinKey)) {
						this.#emitPair([leftRow, rightRow], -1, options.outputChangeSet);
					}
				} else {
					this.#emitPair([leftRow, null], -1, options.outputChangeSet);
				}
				this.#deleteMatchCount(leftRowKey);
			}
		} else {
			// RIGHT SIDE
			const rightRow = options.row as U;

			this.#updateStorage(
				this.#rightStorage,
				{ key: options.joinKey, row: rightRow },
				options.weight
			);

			for (const leftRow of findMatchingRows(this.#leftStorage, options.joinKey)) {
				const leftRowKey = JSON.stringify(leftRow);
				const currentMatchCount = this.#getMatchCount(leftRowKey);

				if (options.weight > 0) {
					// RIGHT ROW ADDED
					if (currentMatchCount === 0) {
						this.#emitPair([leftRow, null], -1, options.outputChangeSet);
					}
					this.#emitPair([leftRow, rightRow], 1, options.outputChangeSet);
					this.#setMatchCount(leftRowKey, currentMatchCount + 1);
				} else {
					// RIGHT ROW REMOVED
					this.#emitPair([leftRow, rightRow], -1, options.outputChangeSet);
					const newMatchCount = currentMatchCount - 1;
					this.#setMatchCount(leftRowKey, newMatchCount);
					if (newMatchCount === 0) {
						this.#emitPair([leftRow, null], 1, options.outputChangeSet);
					}
				}
			}
		}
	}

	#push(changeSet: ChangeSet<T | U>, source: 'left' | 'right') {
		const outputChangeSet = new ChangeSet<[T, U | null]>([]);
		assert(
			source === 'left' || source === 'right',
			Error(`Invalid source: ${source}. Must be 'left' or 'right'`)
		);

		for (const [row, weight] of changeSet.data) {
			const joinKey =
				source === 'left' ? this.#leftKeyExtractor(row as T) : this.#rightKeyExtractor(row as U);

			this.#pushRow({
				row,
				weight,
				joinKey,
				outputChangeSet,
				source
			});
		}

		if (!outputChangeSet.isEmpty()) {
			this.#sink.push(outputChangeSet);
		}
	}

	#processInitialData() {
		// Build hash table from right side
		for (const [row, _weight] of this.#rightSource.pull()) {
			this.#rightStorage.add({
				key: this.#rightKeyExtractor(row),
				row: row
			});
		}

		// Probe with left side
		for (const [leftRow, _weight] of this.#leftSource.pull()) {
			const joinKey = this.#leftKeyExtractor(leftRow);
			const leftRowKey = JSON.stringify(leftRow);

			this.#leftStorage.add({
				key: joinKey,
				row: leftRow
			});

			let matchCount = 0;
			for (const rightRow of findMatchingRows(this.#rightStorage, joinKey)) {
				this.#results.add([leftRow, rightRow]);
				matchCount++;
			}

			if (matchCount === 0) {
				this.#results.add([leftRow, null]);
			}
			this.#setMatchCount(leftRowKey, matchCount);
		}
	}
}
