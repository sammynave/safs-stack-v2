import { assert } from '$lib/asserts.js';
import { BTree, type Comparator } from '../../b-plus-tree/tree.ts';
import { ChangeSet } from '../../change-set/change-set.ts';
import { NullSink } from '../../sinks/null-sink.ts';
import type { Sink, Source } from '../stream-operator-types.ts';

type GroupedRow<T> = {
	keys: (keyof T)[];
	keyValues: Partial<T>;
	rows: BTree<T>;
};

export type JSONValue =
	| string
	| number
	| boolean
	| null
	| JSONValue[]
	| { [key: string]: JSONValue };

type JsonAggGroupResult<T> = Partial<T> & { jsonAgg: JSONValue[] };

/* stateful */
export class JsonAggGroupByOperator<T>
	implements Sink<GroupedRow<T>>, Source<JsonAggGroupResult<T>>
{
	readonly #source: Source<GroupedRow<T>>;
	readonly #column: keyof T | (keyof T)[] | Record<string, keyof T>;
	#sink: Sink<JsonAggGroupResult<T>> | typeof NullSink = NullSink;
	#groupResults: BTree<{ keyValues: Partial<T>; array: JSONValue[] }>;
	#initialized = false;

	constructor(
		source: Source<GroupedRow<T>>,
		column: keyof T | (keyof T)[] | Record<string, keyof T>
	) {
		this.#source = source;
		this.#column = column;
		this.#groupResults = new BTree(this.#createGroupResultComparator());
		this.#source.setSink(this);
	}

	get size() {
		return this.#groupResults.size;
	}

	setSink(newSink: Sink<JsonAggGroupResult<T>>) {
		assert(
			this.#sink === NullSink,
			Error(`Sink already set! Only 1 sink allowed`, { cause: this.#sink })
		);
		this.#sink = newSink;
	}

	push(changeSet: ChangeSet<GroupedRow<T>>) {
		const outputChangeSet = new ChangeSet<JsonAggGroupResult<T>>([]);

		for (const [group, weight] of changeSet.data) {
			if (weight === 0) continue;

			// Find existing entry in the BTree
			const probe = { keyValues: group.keyValues, array: [] };
			const current = this.#groupResults.get(probe);

			// Retract old state if it existed
			if (current) {
				outputChangeSet.append([
					{ ...current.keyValues, jsonAgg: [...current.array] } as JsonAggGroupResult<T>,
					-1
				]);
			}

			// Compute new array for this group
			const newArray = this.#computeArray(group.rows);

			// Always update if group has rows
			if (group.rows.size > 0) {
				const newEntry = { keyValues: group.keyValues, array: newArray };
				this.#groupResults.add(newEntry);
				outputChangeSet.append([
					{ ...group.keyValues, jsonAgg: [...newArray] } as JsonAggGroupResult<T>,
					1
				]);
			} else if (current) {
				// Remove from storage if group is now empty
				this.#groupResults.delete(current);
			}
		}

		if (!outputChangeSet.isEmpty()) {
			this.#sink.push(outputChangeSet);
		}
	}

	*pull() {
		if (!this.#initialized) {
			// clear any pushes we got before initial pull
			this.#groupResults.clear();
			this.#initialized = true;
		}

		// Always re-process from source to get current state
		this.#groupResults.clear();
		for (const [group, weight] of (this.#source as any).pullRaw()) {
			this.#processGroup(group, weight);
		}

		for (const { keyValues, array } of this.#groupResults.values()) {
			yield [{ ...keyValues, jsonAgg: [...array] } as JsonAggGroupResult<T>, 1] as [
				JsonAggGroupResult<T>,
				number
			];
		}
	}

	disconnect() {
		this.#source.disconnect(this as any);
	}

	#processGroup(group: GroupedRow<T>, weight: number) {
		const probe = { keyValues: group.keyValues, array: [] };
		const current = this.#groupResults.get(probe);
		const newArray = this.#computeArray(group.rows);

		if (weight > 0 && group.rows.size > 0) {
			const newEntry = {
				keyValues: group.keyValues,
				array: newArray
			};
			this.#groupResults.add(newEntry);
		} else if (current) {
			// If group still has data, update it; otherwise remove it
			if (group.rows.size > 0) {
				const newEntry = { keyValues: group.keyValues, array: newArray };
				this.#groupResults.add(newEntry);
			} else {
				this.#groupResults.delete(current);
			}
		}
	}

	#computeArray(rows: BTree<T>): JSONValue[] {
		const result: JSONValue[] = [];

		for (const row of rows.values()) {
			// Single column: aggregate values directly
			if (typeof this.#column === 'string' || typeof this.#column === 'symbol') {
				const value = row[this.#column as keyof T];
				if (value !== null && value !== undefined) {
					result.push(value as JSONValue);
				}
			}
			// Array of columns: build object with column names as keys
			else if (Array.isArray(this.#column)) {
				const obj: Record<string, JSONValue> = {};
				for (const col of this.#column) {
					const value = row[col];
					if (value !== null && value !== undefined) {
						// Extract the key name from the column (e.g., 'reactionCounts.emoji' -> 'emoji')
						const key = String(col).includes('.') ? String(col).split('.').pop()! : String(col);
						obj[key] = value as JSONValue;
					}
				}
				// Only add if object has at least one property
				if (Object.keys(obj).length > 0) {
					result.push(obj);
				}
			}
			// Record of columns: build object with custom keys
			else {
				const obj: Record<string, JSONValue> = {};
				for (const [key, col] of Object.entries(this.#column as Record<string, keyof T>)) {
					const value = row[col];
					if (value !== null && value !== undefined) {
						obj[key] = value as JSONValue;
					}
				}
				// Only add if object has at least one property
				if (Object.keys(obj).length > 0) {
					result.push(obj);
				}
			}
		}

		return result;
	}

	#createGroupResultComparator(): Comparator<{ keyValues: Partial<T>; array: JSONValue[] }> {
		return (a, b) => {
			const keyA = JSON.stringify(a.keyValues);
			const keyB = JSON.stringify(b.keyValues);
			if (keyA < keyB) return -1;
			if (keyA > keyB) return 1;
			return 0;
		};
	}
}
