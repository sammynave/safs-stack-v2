export type LockMode = 'exclusive' | 'shared';

export interface MutexLockConfig {
	databasePath: string;
	mode?: LockMode;
}

export const mutexLock = async <T>(
	config: MutexLockConfig,
	mutation: () => Promise<T>
): Promise<T> => {
	if (!('locks' in navigator)) {
		return await mutation();
	}

	const lockIdentifier = `_safs_stack_db_mutation_(${config.databasePath})`;
	const lockMode = config.mode ?? 'exclusive';

	return await navigator.locks.request(lockIdentifier, { mode: lockMode }, async () => {
		return await mutation();
	});
};

export const isLockSupported = (): boolean => {
	return typeof navigator !== 'undefined' && 'locks' in navigator;
};
