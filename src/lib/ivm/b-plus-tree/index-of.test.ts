import { describe, it, expect } from 'vitest';
import { indexOf } from './index-of.ts';

describe('indexOf', () => {
	const numComparator = (a: number, b: number) => a - b;
	const strComparator = (a: string, b: string) => a.localeCompare(b);

	describe('Basic Functionality', () => {
		it('should find element at beginning', () => {
			const keys = [1, 2, 3, 4, 5];
			expect(indexOf(1, keys, 0, numComparator)).toBe(0);
		});

		it('should find element in middle', () => {
			const keys = [1, 2, 3, 4, 5];
			expect(indexOf(3, keys, 0, numComparator)).toBe(2);
		});

		it('should find element at end', () => {
			const keys = [1, 2, 3, 4, 5];
			expect(indexOf(5, keys, 0, numComparator)).toBe(4);
		});

		it('should return insertion index for missing element with failXor=0', () => {
			const keys = [1, 3, 5, 7, 9];
			expect(indexOf(4, keys, 0, numComparator)).toBe(2); // Should insert at index 2
			expect(indexOf(0, keys, 0, numComparator)).toBe(0); // Before all
			expect(indexOf(10, keys, 0, numComparator)).toBe(5); // After all
		});

		it('should handle empty array', () => {
			expect(indexOf(1, [], 0, numComparator)).toBe(0);
		});
	});

	describe('Edge Cases', () => {
		it('should handle single element array - found', () => {
			expect(indexOf(5, [5], 0, numComparator)).toBe(0);
		});

		it('should handle single element array - not found (before)', () => {
			expect(indexOf(3, [5], 0, numComparator)).toBe(0);
		});

		it('should handle single element array - not found (after)', () => {
			expect(indexOf(7, [5], 0, numComparator)).toBe(1);
		});

		it('should handle two element array', () => {
			const keys = [1, 3];
			expect(indexOf(1, keys, 0, numComparator)).toBe(0);
			expect(indexOf(3, keys, 0, numComparator)).toBe(1);
			expect(indexOf(2, keys, 0, numComparator)).toBe(1); // Between
			expect(indexOf(0, keys, 0, numComparator)).toBe(0); // Before
			expect(indexOf(4, keys, 0, numComparator)).toBe(2); // After
		});

		it('should handle large arrays', () => {
			const keys = Array.from({ length: 1000 }, (_, i) => i * 2);
			expect(indexOf(500, keys, 0, numComparator)).toBe(250);
			expect(indexOf(999, keys, 0, numComparator)).toBe(500); // Odd number, not found
		});
	});

	describe('failXor Behavior', () => {
		it('should apply failXor when element not found', () => {
			const keys = [1, 3, 5, 7, 9];
			// Element 4 would be at index 2
			expect(indexOf(4, keys, 0, numComparator)).toBe(2); // 2 ^ 0 = 2
			expect(indexOf(4, keys, 1, numComparator)).toBe(3); // 2 ^ 1 = 3
			expect(indexOf(4, keys, 2, numComparator)).toBe(0); // 2 ^ 2 = 0
		});

		it('should NOT apply failXor when element is found', () => {
			const keys = [1, 3, 5, 7, 9];
			expect(indexOf(5, keys, 0, numComparator)).toBe(2);
			expect(indexOf(5, keys, 1, numComparator)).toBe(2); // Still 2, not 3
			expect(indexOf(5, keys, 255, numComparator)).toBe(2); // Still 2
		});

		it('should handle failXor at boundaries', () => {
			const keys = [2, 4, 6];
			expect(indexOf(1, keys, 1, numComparator)).toBe(1); // 0 ^ 1 = 1
			expect(indexOf(7, keys, 1, numComparator)).toBe(2); // 3 ^ 1 = 2
		});
	});

	describe('Duplicate Handling', () => {
		it('should return valid index for duplicates', () => {
			const keys = [1, 2, 2, 2, 3];
			const result = indexOf(2, keys, 0, numComparator);
			// Should return one of the valid indices: 1, 2, or 3
			expect(result).toBeGreaterThanOrEqual(1);
			expect(result).toBeLessThanOrEqual(3);
			expect(keys[result]).toBe(2);
		});

		it('should handle all duplicates', () => {
			const keys = [5, 5, 5, 5];
			const result = indexOf(5, keys, 0, numComparator);
			expect(result).toBeGreaterThanOrEqual(0);
			expect(result).toBeLessThanOrEqual(3);
		});
	});

	describe('NaN Handling', () => {
		it('should throw when NaN is used as search key', () => {
			const keys = [1, 2, 3];
			expect(() => indexOf(NaN, keys, 0, numComparator)).toThrow('NaN was used as a key');
		});

		it('should handle NaN in comparator result', () => {
			const keys = [1, 2, 3];
			const nanComparator = () => NaN;
			// Should return keys.length when comparator returns NaN
			expect(indexOf(2, keys, 0, nanComparator)).toBe(3);
		});
	});

	describe('Different Comparators', () => {
		it('should work with string comparator', () => {
			const keys = ['apple', 'banana', 'cherry', 'date'];
			expect(indexOf('banana', keys, 0, strComparator)).toBe(1);
			expect(indexOf('blueberry', keys, 0, strComparator)).toBe(2);
		});

		it('should work with object comparator', () => {
			type User = { id: number; age: number };
			const ageComparator = (a: User, b: User) => a.age - b.age;
			const users: User[] = [
				{ id: 1, age: 20 },
				{ id: 2, age: 25 },
				{ id: 3, age: 30 }
			];
			expect(indexOf({ id: 0, age: 25 }, users, 0, ageComparator)).toBe(1);
			expect(indexOf({ id: 0, age: 27 }, users, 0, ageComparator)).toBe(2);
		});

		it('should work with reverse comparator', () => {
			const reverseComparator = (a: number, b: number) => b - a;
			const keys = [9, 7, 5, 3, 1]; // Descending order
			expect(indexOf(5, keys, 0, reverseComparator)).toBe(2);
			expect(indexOf(6, keys, 0, reverseComparator)).toBe(2);
		});
	});

	describe('Boundary Conditions', () => {
		it('should handle search less than all elements', () => {
			const keys = [10, 20, 30];
			expect(indexOf(5, keys, 0, numComparator)).toBe(0);
		});

		it('should handle search greater than all elements', () => {
			const keys = [10, 20, 30];
			expect(indexOf(35, keys, 0, numComparator)).toBe(3);
		});

		it('should handle negative numbers', () => {
			const keys = [-10, -5, 0, 5, 10];
			expect(indexOf(-5, keys, 0, numComparator)).toBe(1);
			expect(indexOf(-7, keys, 0, numComparator)).toBe(1);
		});
	});

	describe('Stress Tests', () => {
		it('should handle very large arrays efficiently', () => {
			const keys = Array.from({ length: 100000 }, (_, i) => i);
			expect(indexOf(50000, keys, 0, numComparator)).toBe(50000);
			expect(indexOf(99999, keys, 0, numComparator)).toBe(99999);
			expect(indexOf(0, keys, 0, numComparator)).toBe(0);
		});

		it('should handle sparse arrays (gaps)', () => {
			const keys = [0, 100, 200, 300, 400];
			for (let i = 1; i < 100; i++) {
				expect(indexOf(i, keys, 0, numComparator)).toBe(1);
			}
		});
	});
});
