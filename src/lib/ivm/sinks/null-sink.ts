import type { ChangeSet } from '../change-set/change-set.ts';

export const NullSink = {
	push(_changeSet: ChangeSet<unknown>): void {
		throw Error('Sink is not set!');
	}
};
