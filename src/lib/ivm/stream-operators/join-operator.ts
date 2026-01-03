import { assert } from '$lib/asserts.js';
import { BTree, type Comparator } from '../b-plus-tree/tree.ts';
import { ChangeSet } from '../change-set/change-set.ts';
import { NullSink } from '../sinks/null-sink.ts';
import {
	compareKeys,
	compareStrings,
	createIndexedComparator,
	findMatchingRows
} from './join-utils.ts';
import type { Sink, Source } from './stream-operator-types.ts';

type IndexedRow<R> = {
	key: unknown; // The join key
	row: R; // The actual row data
};
/* stateful */
export class JoinOperator<T, U> implements Sink<[T, U]>, Source<[T, U]> {
	readonly #leftSource: Source<T>;
	readonly #rightSource: Source<U>;
	readonly #leftKeyExtractor: (row: T) => unknown;
	readonly #rightKeyExtractor: (row: U) => unknown;
	#sink: Sink<[T, U]> | typeof NullSink = NullSink;
	#results: BTree<[T, U]>;
	#leftStorage: BTree<IndexedRow<T>>;
	#rightStorage: BTree<IndexedRow<U>>;

	constructor(
		leftSource: Source<T>,
		rightSource: Source<U>,
		leftKeyExtractor: (row: T) => unknown,
		rightKeyExtractor: (row: U) => unknown,
		resultComparator: Comparator<[T, U]>
	) {
		this.#leftSource = leftSource;
		this.#rightSource = rightSource;
		this.#leftKeyExtractor = leftKeyExtractor;
		this.#rightKeyExtractor = rightKeyExtractor;

		// Initialize storage BTrees with composite comparators
		this.#leftStorage = new BTree<IndexedRow<T>>(createIndexedComparator<T>());
		this.#rightStorage = new BTree<IndexedRow<U>>(createIndexedComparator<U>());

		this.#results = new BTree<[T, U]>(resultComparator);

		// Load initial data and perform initial join
		this.#processInitialData();

		// Set up sinks with source identification
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

	setSink(sink: Sink<[T, U]>) {
		assert(
			this.#sink === NullSink,
			Error(`Sink already set! Only 1 sink allowed`, { cause: this.#sink })
		);
		this.#sink = sink;
	}

	push() {
		throw Error('Can not push to JoinOperator directly. Use pushLeft or pushRight');
	}

	pushLeft(changeSet: ChangeSet<T | U>) {
		this.#push(changeSet, 'left');
	}

	pushRight(changeSet: ChangeSet<T | U>) {
		this.#push(changeSet, 'right');
	}

	*pull() {
		for (const joinedRow of this.#results.values()) {
			yield [joinedRow, 1] as [[T, U], number];
		}
	}

	disconnect() {
		this.#leftSource.disconnect(this);
		this.#rightSource.disconnect(this);
	}

	#pushRow(options: {
		storage: BTree<IndexedRow<T | U>>;
		otherStorage: BTree<IndexedRow<T | U>>;
		row: T | U;
		weight: number;
		joinKey: string;
		outputChangeSet: ChangeSet<[T, U]>;
		source: 'left' | 'right';
	}) {
		// Add to storage
		if (options.weight > 0) {
			options.storage.add({ key: options.joinKey, row: options.row });
		} else if (options.weight < 0) {
			options.storage.delete({ key: options.joinKey, row: options.row });
		} else {
			throw Error('Weight is 0');
		}

		// Find matching rows in otherStorage storage
		for (const otherRow of findMatchingRows(options.otherStorage, options.joinKey)) {
			const joined =
				options.source === 'left'
					? ([options.row as T, otherRow as U] satisfies [T, U])
					: ([otherRow as T, options.row as U] satisfies [T, U]);
			const otherWeight = this.#getRowWeight(otherRow);
			const joinedWeight = options.weight * otherWeight;

			options.outputChangeSet.append([joined, joinedWeight]);

			// Update results storage
			if (options.weight > 0 && otherWeight > 0) {
				this.#results.add(joined);
			} else if (options.weight < 0 || otherWeight < 0) {
				this.#results.delete(joined);
			}
		}
	}

	#push(changeSet: ChangeSet<T | U>, source: 'left' | 'right') {
		const outputChangeSet = new ChangeSet<[T, U]>([]);
		assert(
			source === 'left' || source === 'right',
			Error(`Invalid source: ${source}. Must be 'left' or 'right'`)
		);
		for (const [row, weight] of changeSet.data) {
			const options =
				source === 'left'
					? {
							storage: this.#leftStorage,
							otherStorage: this.#rightStorage,
							row,
							weight,
							joinKey: this.#leftKeyExtractor(row),
							outputChangeSet,
							source: 'left'
						}
					: {
							storage: this.#rightStorage,
							otherStorage: this.#leftStorage,
							row,
							weight,
							joinKey: this.#rightKeyExtractor(row),
							outputChangeSet,
							source: 'right'
						};
			this.#pushRow(options);
		}

		if (!outputChangeSet.isEmpty()) {
			this.#sink.push(outputChangeSet);
		}
	}

	#getRowWeight<R>(row: R): number {
		/*
		 * This is an artifact of the initial version that tried to adhere to the DBSP paper
		 * We don't really need to support multi-sets/bags right now. It would be nice for
		 * analyitcs style tables (no primary key for example).
		 * The one actual use that we might want someday is supporting recursive queries that accumulate duplicates
		 *
		 * WITH RECURSIVE paths AS (
		 *		SELECT source, dest, 1 as count FROM edges
		 *		UNION ALL
		 *		SELECT p.source, e.dest, p.count + 1
		 *		FROM paths p JOIN edges e ON p.dest = e.source
		 *		WHERE p.count < 10
		 * )
		 * SELECT * FROM paths;  -- Without DISTINCT
		 *
		 * in this case though, when would we not want DISTINCT?
		 * probably not that often but who knows? let's just deal with it
		 * in user land if it comes up
		 */
		return 1;
	}

	#processInitialData() {
		const config =
			this.#leftSource.size > this.#rightSource.size
				? {
						buildSource: this.#leftSource,
						buildStorage: this.#leftStorage,
						buildKeyExtractor: this.#leftKeyExtractor,
						probeSource: this.#rightSource,
						probeStorage: this.#rightStorage,
						probeKeyExtractor: this.#rightKeyExtractor,
						probeSide: 'right'
					}
				: {
						buildSource: this.#rightSource,
						buildStorage: this.#rightStorage,
						buildKeyExtractor: this.#rightKeyExtractor,
						probeSource: this.#leftSource,
						probeStorage: this.#leftStorage,
						probeKeyExtractor: this.#leftKeyExtractor,
						probeSide: 'left'
					};

		this.#buildAndProbe(config);
	}

	#buildAndProbe(config: {
		buildSource: Source<T> | Source<U>;
		buildStorage: BTree<IndexedRow<T>> | BTree<IndexedRow<U>>;
		buildKeyExtractor: (row: T | U) => unknown;
		probeSource: Source<T> | Source<U>;
		probeStorage: BTree<IndexedRow<T>> | BTree<IndexedRow<U>>;
		probeKeyExtractor: (row: T | U) => unknown;
		probeSide: 'left' | 'right';
	}) {
		for (const [row, _weight] of config.buildSource.pull()) {
			(config.buildStorage as BTree<IndexedRow<T | U>>).add({
				key: config.buildKeyExtractor(row),
				row: row
			});
		}

		for (const [probeRow, _weight] of config.probeSource.pull()) {
			const joinKey = config.probeKeyExtractor(probeRow);
			(config.probeStorage as BTree<IndexedRow<T | U>>).add({
				key: joinKey,
				row: probeRow
			});

			for (const buildRow of findMatchingRows(
				config.buildStorage as BTree<IndexedRow<T | U>>,
				joinKey
			)) {
				const joined: [T, U] =
					config.probeSide === 'left'
						? [probeRow as T, buildRow as U]
						: [buildRow as T, probeRow as U];
				this.#results.add(joined);
			}
		}
	}
}
