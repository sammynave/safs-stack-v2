export interface CommutativeGroup<T> {
	zero(): T;
	add(a: T, b: T): T;
	subtract(a: T, b: T): T;
	negate(a: T): T;
}
