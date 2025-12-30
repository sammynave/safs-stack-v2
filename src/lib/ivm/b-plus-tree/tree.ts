import { indexOf } from './index-of.ts';
import { BTreeInternalNode, BTreeNode } from './node.ts';

export type Comparator<T> = (a: T, b: T) => number;

const emptyLeaf = new BTreeNode<any>([], true);
emptyLeaf.isShared = true;
/* this is stolen from zql https://github.com/rocicorp/mono/tree/main/packages/zql */
export class BTree<T> {
	#root: BTreeNode<T> = emptyLeaf as BTreeNode<T>;
	comparator: Comparator<T>;
	size: number = 0;

	constructor(comparator: Comparator<T>) {
		this.comparator = comparator;
	}

	/** Releases the tree so that its size is 0. */
	clear() {
		this.#root = emptyLeaf as BTreeNode<T>;
		this.size = 0;
	}

	get(key: T): T | undefined {
		return this.#root.get(key, this);
	}

	add(key: T): void {
		if (this.#root.isShared) this.#root = this.#root.clone();
		const result = this.#root.set(key, this);
		if (result === null) return;
		// Root node has split, so create a new root node.
		this.#root = new BTreeInternalNode<T>([this.#root, result]);
	}

	has(key: T): boolean {
		return this.#root.has(key, this);
	}

	delete(key: T): boolean {
		let root = this.#root;
		if (root.isShared) {
			this.#root = root = root.clone();
		}
		try {
			return root.delete(key, this);
		} finally {
			let isShared;
			while (root.keys.length <= 1 && root.isInternal()) {
				isShared ||= root.isShared;
				this.#root = root =
					root.keys.length === 0 ? emptyLeaf : (root as BTreeInternalNode<T>).children[0];
			}
			// If any ancestor of the new root was shared, the new root must also be shared
			if (isShared) {
				root.isShared = true;
			}
		}
	}

	// Range query methods

	/**
	 * Returns an iterator over all items in the tree.
	 */
	values(): IterableIterator<T> {
		return this.#valuesFrom(undefined, true);
	}

	/**
	 * Returns an iterator over items starting from the specified item.
	 * @param lowestItem The lowest item to start iteration from (undefined starts from the beginning)
	 * @param inclusive Whether to include the lowestItem if it exists (default: true)
	 */
	valuesFrom(lowestItem?: T, inclusive: boolean = true): IterableIterator<T> {
		return this.#valuesFrom(lowestItem, inclusive);
	}

	/**
	 * Returns an iterator over all items in reverse order.
	 */
	valuesReversed(): IterableIterator<T> {
		return this.#valuesFromReversed(undefined, true);
	}

	/**
	 * Returns an iterator over items in reverse order, starting from the specified item.
	 * @param highestItem The highest item to start iteration from (undefined starts from the end)
	 * @param inclusive Whether to include the highestItem if it exists (default: true)
	 */
	valuesFromReversed(highestItem?: T, inclusive: boolean = true): IterableIterator<T> {
		return this.#valuesFromReversed(highestItem, inclusive);
	}

	/**
	 * Makes the tree iterable with for...of loops.
	 */
	[Symbol.iterator](): IterableIterator<T> {
		return this.values();
	}

	// Helper methods for range queries

	#valuesFrom(lowestItem: T | undefined, inclusive: boolean): IterableIterator<T> {
		const info = this.#findPath(lowestItem, this.#root);
		if (info === undefined) {
			return this.#iterator<T>(() => ({ done: true, value: undefined as any }));
		}

		let [nodeQueue, nodeIndex, leaf] = info;
		let i = lowestItem === undefined ? -1 : indexOf(lowestItem, leaf.keys, 0, this.comparator) - 1;

		if (
			!inclusive &&
			i < leaf.keys.length &&
			this.comparator(leaf.keys[i + 1], lowestItem!) === 0
		) {
			i++;
		}

		return this.#iterator<T>(() => {
			for (;;) {
				if (++i < leaf.keys.length) {
					const item = leaf.keys[i];
					return { done: false, value: item };
				}

				let level = -1;
				for (;;) {
					if (++level >= nodeQueue.length) {
						return { done: true, value: undefined as any };
					}
					if (++nodeIndex[level] < nodeQueue[level].length) {
						break;
					}
				}
				for (; level > 0; level--) {
					nodeQueue[level - 1] = (
						nodeQueue[level][nodeIndex[level]] as BTreeInternalNode<T>
					).children;
					nodeIndex[level - 1] = 0;
				}
				leaf = nodeQueue[0][nodeIndex[0]];
				i = -1;
			}
		});
	}

	#valuesFromReversed(highestItem: T | undefined, inclusive: boolean): IterableIterator<T> {
		const maxItem = this.#getMaxItem();
		if (highestItem === undefined) {
			highestItem = maxItem;
			if (highestItem === undefined) {
				return this.#iterator<T>(() => ({ done: true, value: undefined as any }));
			}
		}

		const info =
			this.#findPath(highestItem, this.#root) ||
			(maxItem ? this.#findPath(maxItem, this.#root) : undefined);
		if (!info) {
			return this.#iterator<T>(() => ({ done: true, value: undefined as any }));
		}

		let [nodeQueue, nodeIndex, leaf] = info;
		let i = indexOf(highestItem, leaf.keys, 0, this.comparator);
		if (inclusive && i < leaf.keys.length && this.comparator(leaf.keys[i], highestItem) <= 0) {
			i++;
		}

		return this.#iterator<T>(() => {
			for (;;) {
				if (--i >= 0) {
					const item = leaf.keys[i];
					return { done: false, value: item };
				}

				let level = -1;
				for (;;) {
					if (++level >= nodeQueue.length) {
						return { done: true, value: undefined as any };
					}
					if (--nodeIndex[level] >= 0) {
						break;
					}
				}
				for (; level > 0; level--) {
					nodeQueue[level - 1] = (
						nodeQueue[level][nodeIndex[level]] as BTreeInternalNode<T>
					).children;
					nodeIndex[level - 1] = nodeQueue[level - 1].length - 1;
				}
				leaf = nodeQueue[0][nodeIndex[0]];
				i = leaf.keys.length;
			}
		});
	}

	#findPath(
		item: T | undefined,
		root: BTreeNode<T>
	): [nodeQueue: BTreeNode<T>[][], nodeIndex: number[], leaf: BTreeNode<T>] | undefined {
		let nextNode = root;
		const nodeQueue: BTreeNode<T>[][] = [];
		const nodeIndex: number[] = [];

		if (nextNode.isInternal()) {
			for (let d = 0; nextNode.isInternal(); d++) {
				nodeQueue[d] = (nextNode as BTreeInternalNode<T>).children;
				nodeIndex[d] = item === undefined ? 0 : indexOf(item, nextNode.keys, 0, this.comparator);
				if (nodeIndex[d] >= nodeQueue[d].length) return; // first item > maxItem()
				nextNode = nodeQueue[d][nodeIndex[d]];
			}
			nodeQueue.reverse();
			nodeIndex.reverse();
		}
		return [nodeQueue, nodeIndex, nextNode];
	}

	#getMaxItem(): T | undefined {
		return this.#root.maxKey();
	}

	#iterator<U>(next: () => IteratorResult<U>): IterableIterator<U> {
		return {
			next,
			[Symbol.iterator]() {
				return this;
			}
		};
	}
}
