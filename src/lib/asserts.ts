export function assert(bool: boolean, error: Error) {
	if (!bool) {
		throw error;
	}
}
