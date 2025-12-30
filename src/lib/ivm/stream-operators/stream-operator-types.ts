import type { ChangeSet } from '../change-set/change-set.ts';

export interface Source<T> {
	size: number;
	setSink: (
		sink: Sink<T>,
		comparator?: (a: unknown, b: unknown) => 0 | 1 | -1,
		sort?: [string, 'asc' | 'desc']
	) => void;
	pull: () => Generator<[T, number]>;
	disconnect: (() => void) | ((sink: Sink<T>) => void);
}

export interface Sink<T> {
	push: (changeSet: ChangeSet<T>) => void;
}
