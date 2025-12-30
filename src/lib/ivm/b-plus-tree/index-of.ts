import type { Comparator } from './tree.ts';
/**
 * Binary search algorithm that finds the index of a key in a sorted array.
 *
 * This implementation uses XOR-based encoding to distinguish between
 * "found" and "not found" cases in a single return value, avoiding the need
 * for tuple returns or object allocations in hot paths.
 *
 * ALGORITHM: Standard binary search with O(log n) time complexity
 * - Maintains search range [lo, hi) using two pointers
 * - Calculates midpoint and compares with target key
 * - Narrows search range based on comparison result
 * - Terminates when range is empty (lo === hi)
 *
 * RETURN VALUE ENCODING:
 * - If key IS found: returns the exact index (0 to keys.length-1)
 * - If key NOT found: returns (insertion_index ^ failXor)
 *
 * FAILXOR PARAMETER USAGE:
 * - failXor = 0: Returns insertion index directly when not found
 *   Example: indexOf(5, [1,3,7,9], 0, cmp) → 2 (whether found or not)
 *   Use case: When you only need the position, don't care about exact match
 *
 * - failXor = 1: Flips LSB of insertion index when not found
 *   Example: indexOf(5, [1,3,7,9], 1, cmp) → 3 (2 ^ 1 = 3)
 *   Use case: Distinguish found (even) vs not-found (odd if insertion point is even)
 *   Caller can check: (result & 1) to detect if XOR was applied
 *
 * NaN HANDLING:
 * This function includes defensive checks for NaN values because:
 * 1. NaN breaks ordering invariants in B+ trees
 * 2. NaN !== NaN, so comparisons behave unexpectedly
 * 3. Allowing NaN keys would corrupt the data structure
 *
 * @param key - The value to search for
 * @param keys - Sorted array to search in (must be sorted according to comparator)
 * @param failXor - XOR mask applied to insertion index when key not found (typically 0 or 1). Callers that don't care whether there was a match will set failXor=0.
 * @param comparator - Comparison function: returns <0 if a<b, >0 if a>b, 0 if equal
 * @returns Index if found, or (insertion_index ^ failXor) if not found.
 * @throws Error if key is NaN
 */

export function indexOf<T>(key: T, keys: T[], failXor: number, comparator: Comparator<T>): number {
	// Initialize search range [lo, hi) - note: hi is exclusive
	let lo = 0;
	let hi = keys.length;
	// Bit shift right by 1 is equivalent to Math.floor(hi / 2)
	let mid = hi >> 1;

	while (lo < hi) {
		// Compare key at midpoint with search key
		const c = comparator(keys[mid], key);
		if (c < 0) {
			// keys[mid] < key, so search in upper half
			lo = mid + 1;
		} else if (c > 0) {
			// keys[mid] > key, so search in lower half
			hi = mid;
		} else if (c === 0) {
			// Exact match found
			return mid;
		} else {
			// c is NaN or otherwise invalid (shouldn't happen with valid comparator)
			// This branch handles edge cases where comparator returns NaN
			if (key === key) {
				// The search key is valid (not NaN), but comparator returned NaN
				// This indicates a problem with the comparator or data in the array
				// Return keys.length as a sentinel value indicating corruption
				return keys.length;
			}
			// The search key itself is NaN (key !== key is true only for NaN)
			// NaN keys are not allowed as they break B+ tree ordering
			throw new Error('NaN was used as a key');
		}
		// Calculate new midpoint for next iteration
		mid = (lo + hi) >> 1;
	}
	// Key not found. At this point, lo === hi === insertion index
	// Apply XOR mask to encode "not found" in the return value
	// - If failXor = 0: returns insertion index unchanged
	// - If failXor = 1: flips LSB (even ↔ odd)
	return mid ^ failXor;
}
