import { indexOf } from './index-of.ts';
import type { BTree } from './tree.ts';

const MAX_NODE_SIZE = 32;

/** Leaf node / base class. **************************************************/
export class BTreeNode<T> {
	// If this is an internal node, keys[i] is the highest key in children[i].
	// If this is a leaf node, keys[i] is the actual item.
	keys: T[];
	// True if this node might be within multiple `BTree`s (or have multiple parents).
	// If so, it must be cloned before being mutated to avoid changing an unrelated tree.
	// This is transitive: if it's true, children are also shared even if `isShared!=true`
	// in those children. (Certain operations will propagate isShared=true to children.)
	isShared: true | undefined;
	leaf: boolean;

	constructor(keys: T[], isLeaf: boolean) {
		this.keys = keys;
		this.leaf = isLeaf;
		this.isShared = undefined;
	}

	isInternal(): this is BTreeInternalNode<T> {
		return !this.leaf;
	}

	maxKey(): T | undefined {
		return this.keys[this.keys.length - 1];
	}

	minKey(): T | undefined {
		return this.keys[0];
	}

	clone(): BTreeNode<T> {
		return new BTreeNode<T>(this.keys.slice(0), this.leaf);
	}

	get(key: T, tree: BTree<T>): T | undefined {
		const i = indexOf(key, this.keys, -1, tree.comparator);
		return i < 0 ? undefined : this.keys[i];
	}

	has(key: T, tree: BTree<T>): boolean {
		const i = indexOf(key, this.keys, -1, tree.comparator);
		return i >= 0 && i < this.keys.length;
	}

	set(key: T, tree: BTree<T>): null | BTreeNode<T> {
		let i = indexOf(key, this.keys, -1, tree.comparator);
		if (i < 0) {
			// key does not exist yet
			i = ~i;
			tree.size++;

			if (this.keys.length < MAX_NODE_SIZE) {
				this.keys.splice(i, 0, key);
				return null;
			}
			// This leaf node is full and must split
			const newRightSibling = this.splitOffRightSide();
			let target: BTreeNode<T> = this;
			if (i > this.keys.length) {
				i -= this.keys.length;
				target = newRightSibling;
			}
			target.keys.splice(i, 0, key);

			return newRightSibling;
		}

		// usually this is a no-op, but some users may wish to edit the key
		this.keys[i] = key;
		return null;
	}

	takeFromRight(rhs: BTreeNode<T>) {
		this.keys.push(rhs.keys.shift()!);
	}

	takeFromLeft(lhs: BTreeNode<T>) {
		this.keys.unshift(lhs.keys.pop()!);
	}

	splitOffRightSide(): BTreeNode<T> {
		const half = this.keys.length >> 1;
		const keys = this.keys.splice(half);
		return new BTreeNode<T>(keys, this.leaf);
	}

	delete(key: T, tree: BTree<T>): boolean {
		const cmp = tree.comparator;
		const iLow = indexOf(key, this.keys, -1, cmp);

		if (iLow < 0) {
			return false;
		}

		const { keys } = this;
		if (this.isShared === true) {
			throw new Error('BTree illegally changed or cloned in delete');
		}

		this.keys.splice(iLow, 1);
		tree.size--;
		return true;
	}

	mergeSibling(rhs: BTreeNode<T>, _: number) {
		this.keys.push(...rhs.keys);
	}
}

/** Internal node (non-leaf node) ********************************************/
/* used as a routing layer from root to leaves */
export class BTreeInternalNode<T> extends BTreeNode<T> {
	children: BTreeNode<T>[];

	/**
	 * This does not mark `children` as shared, so it is the responsibility of the caller
	 * to ensure children are either marked shared, or aren't included in another tree.
	 */
	constructor(children: BTreeNode<T>[], keys?: T[]) {
		if (!keys) {
			keys = [];
			for (let i = 0; i < children.length; i++) {
				keys[i] = children[i].maxKey()!;
			}
		}
		super(keys, false);
		this.children = children;
	}

	clone(): BTreeNode<T> {
		const children = this.children.slice(0);
		for (let i = 0; i < children.length; i++) {
			children[i].isShared = true;
		}
		return new BTreeInternalNode<T>(children, this.keys.slice(0));
	}

	minKey(): T | undefined {
		return this.children[0].minKey();
	}

	get(key: T, tree: BTree<T>): T | undefined {
		const i = indexOf(key, this.keys, 0, tree.comparator);
		const { children } = this;
		return i < children.length ? children[i].get(key, tree) : undefined;
	}

	has(key: T, tree: BTree<T>): boolean {
		const i = indexOf(key, this.keys, 0, tree.comparator);
		const { children } = this;
		return i < children.length ? children[i].has(key, tree) : false;
	}

	set(key: T, tree: BTree<T>): null | BTreeNode<T> {
		const c = this.children;
		const cmp = tree.comparator;
		let i = Math.min(indexOf(key, this.keys, 0, cmp), c.length - 1);
		let child = c[i];

		if (child.isShared) {
			c[i] = child = child.clone();
		}
		if (child.keys.length >= MAX_NODE_SIZE) {
			// child is full; inserting anything else will cause a split.
			// Shifting an item to the left or right sibling may avoid a split.
			// We can do a shift if the adjacent node is not full and if the
			// current key can still be placed in the same node after the shift.
			let other: BTreeNode<T>;
			if (i > 0 && (other = c[i - 1]).keys.length < MAX_NODE_SIZE && cmp(child.keys[0], key) < 0) {
				if (other.isShared) {
					c[i - 1] = other = other.clone();
				}
				other.takeFromRight(child);
				this.keys[i - 1] = other.maxKey()!;
			} else if (
				(other = c[i + 1]) !== undefined &&
				other.keys.length < MAX_NODE_SIZE &&
				cmp(child.maxKey()!, key) < 0
			) {
				if (other.isShared) c[i + 1] = other = other.clone();
				other.takeFromLeft(child);
				this.keys[i] = c[i].maxKey()!;
			}
		}

		const result = child.set(key, tree);
		this.keys[i] = child.maxKey()!;
		if (result === null) return null;

		// The child has split and `result` is a new right child... does it fit?
		if (this.keys.length < MAX_NODE_SIZE) {
			// yes
			this.insert(i + 1, result);
			return null;
		}
		// no, we must split also
		const newRightSibling = this.splitOffRightSide();
		let target: BTreeInternalNode<T> = this;
		if (cmp(result.maxKey()!, this.maxKey()!) > 0) {
			target = newRightSibling;
			i -= this.keys.length;
		}
		target.insert(i + 1, result);
		return newRightSibling;
	}

	/**
	 * Inserts `child` at index `i`.
	 * This does not mark `child` as shared, so it is the responsibility of the caller
	 * to ensure that either child is marked shared, or it is not included in another tree.
	 */
	insert(i: number, child: BTreeNode<T>) {
		this.children.splice(i, 0, child);
		this.keys.splice(i, 0, child.maxKey()!);
	}

	/**
	 * Split this node.
	 * Modifies this to remove the second half of the items, returning a separate node containing them.
	 */
	splitOffRightSide(): BTreeInternalNode<T> {
		const half = this.children.length >> 1;
		return new BTreeInternalNode<T>(this.children.splice(half), this.keys.splice(half));
	}

	takeFromRight(rhs: BTreeNode<T>) {
		this.keys.push(rhs.keys.shift()!);
		this.children.push((rhs as BTreeInternalNode<T>).children.shift()!);
	}

	takeFromLeft(lhs: BTreeNode<T>) {
		this.keys.unshift(lhs.keys.pop()!);
		this.children.unshift((lhs as BTreeInternalNode<T>).children.pop()!);
	}

	delete(key: T, tree: BTree<T>): boolean {
		const cmp = tree.comparator;
		const { keys } = this;
		const { children } = this;
		let iLow = indexOf(key, this.keys, 0, cmp);
		let i = iLow;
		const iHigh = Math.min(iLow, keys.length - 1);
		if (i <= iHigh) {
			try {
				if (children[i].isShared) {
					children[i] = children[i].clone();
				}
				const result = children[i].delete(key, tree);
				// Note: if children[i] is empty then keys[i]=undefined.
				//       This is an invalid state, but it is fixed below.
				keys[i] = children[i].maxKey()!;
				return result;
			} finally {
				// Deletions may have occurred, so look for opportunities to merge nodes.
				const half = MAX_NODE_SIZE >> 1;
				if (iLow > 0) iLow--;
				for (i = iHigh; i >= iLow; i--) {
					if (children[i].keys.length <= half) {
						if (children[i].keys.length !== 0) {
							this.tryMerge(i, MAX_NODE_SIZE);
						} else {
							// child is empty! delete it!
							keys.splice(i, 1);
							children.splice(i, 1);
						}
					}
				}
			}
		}
		return false;
	}

	/** Merges child i with child i+1 if their combined size is not too large */
	tryMerge(i: number, maxSize: number): boolean {
		const { children } = this;
		if (i >= 0 && i + 1 < children.length) {
			if (children[i].keys.length + children[i + 1].keys.length <= maxSize) {
				if (children[i].isShared)
					// cloned already UNLESS i is outside scan range
					children[i] = children[i].clone();
				children[i].mergeSibling(children[i + 1], maxSize);
				children.splice(i + 1, 1);
				this.keys.splice(i + 1, 1);
				this.keys[i] = children[i].maxKey()!;
				return true;
			}
		}
		return false;
	}

	/**
	 * Move children from `rhs` into this.
	 * `rhs` must be part of this tree, and be removed from it after this call
	 * (otherwise isShared for its children could be incorrect).
	 */
	mergeSibling(rhs: BTreeNode<T>, maxNodeSize: number) {
		const oldLength = this.keys.length;
		this.keys.push(...rhs.keys);
		const rhsChildren = (rhs as BTreeInternalNode<T>).children;
		this.children.push(...rhsChildren);

		if (rhs.isShared && !this.isShared) {
			// All children of a shared node are implicitly shared, and since their new
			// parent is not shared, they must now be explicitly marked as shared.
			for (let i = 0; i < rhsChildren.length; i++) {
				rhsChildren[i].isShared = true;
			}
		}

		// If our children are themselves almost empty due to a mass-delete,
		// they may need to be merged too (but only the oldLength-1 and its
		// right sibling should need this).
		this.tryMerge(oldLength - 1, maxNodeSize);
	}
}
