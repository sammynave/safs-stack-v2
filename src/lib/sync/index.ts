/**
 * sync-lib - Framework-agnostic CRDT sync library for vanilla SQLite
 *
 * Provides three-layer synchronization:
 * - UI Layer: Reactive repositories with command binding
 * - Tab Layer: Cross-tab/window sync via BroadcastChannel
 * - Peer Layer: Remote peer sync via WebSocket
 */

// Main exports
export { Syncer } from './syncer.ts';
export { DatabaseConnection } from './database-connection.ts';

// Sync layers
export { Ui, ReactiveRepo } from './syncers/ui.ts';
export { Tab } from './syncers/tab.ts';
export { Peer } from './syncers/peer.ts';

// Transport
export { WsTransport, type WebSocketLike } from './transports/ws-transport.ts';

// Types
export type {
	Change,
	Changes,
	MessagePayload,
	MessageData,
	DatabaseInterface,
	PreparedStatement,
	Transport,
	TransportHandlers,
	RepoDefinition,
	BoundCommand,
	BoundCommands,
	DatabaseConnectionConfig,
	SyncEventData
} from './types.ts';

export { CrdtEvent, MessageType, Source } from './types.ts';

// Utils
export {
	encode,
	decode,
	generateSiteId,
	serializeSet,
	deserializeSet,
	pkToHex,
	hexToPk,
	hexToBytes,
	bytesToHex
} from './utils.ts';

// Schema and queries (for advanced usage)
export {
	INIT_STATEMENTS,
	CREATE_SITE_ID_TABLE,
	CREATE_DB_VERSION_TABLE,
	CREATE_CHANGES_TABLE,
	CREATE_TRACKED_PEERS_TABLE,
	generateInsertTrigger,
	generateUpdateTrigger,
	generateDeleteTrigger
} from './schema.ts';

export {
	SELECT_VERSION,
	SELECT_SITE_ID,
	SELECT_CHANGES_SINCE,
	SELECT_CLIENT_CHANGES_SINCE,
	SELECT_LAST_TRACKED_VERSION_FOR_PEER,
	SELECT_NON_CLIENT_CHANGES,
	INSERT_CHANGES,
	UPSERT_TRACKED_PEER
} from './queries.ts';
