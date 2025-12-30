import type { CommutativeGroup } from './change-set-types.ts';

export class BilinearChangeSetAlgebra<A, B> implements CommutativeGroup<[A, B]> {
	#groupA: CommutativeGroup<A>;
	#groupB: CommutativeGroup<B>;
	constructor(groupA: CommutativeGroup<A>, groupB: CommutativeGroup<B>) {
		this.#groupA = groupA;
		this.#groupB = groupB;
	}

	zero(): [A, B] {
		return [this.#groupA.zero(), this.#groupB.zero()];
	}

	add(a: [A, B], b: [A, B]): [A, B] {
		return [this.#groupA.add(a[0], b[0]), this.#groupB.add(a[1], b[1])];
	}

	subtract(a: [A, B], b: [A, B]): [A, B] {
		return [this.#groupA.subtract(a[0], b[0]), this.#groupB.subtract(a[1], b[1])];
	}

	negate(a: [A, B]): [A, B] {
		return [this.#groupA.negate(a[0]), this.#groupB.negate(a[1])];
	}
}
