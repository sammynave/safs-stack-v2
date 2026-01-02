/**
 * Core types for CRDT sync library
 */

/**
 * A single change record representing a modification to a table/column
 * Format: [table, pk, cid, val, col_version, db_version, site_id, cl, seq]
 */
export type Change = [
  table: string,
  pk: string, // hex-encoded primary key
  cid: string, // column id, or "-1" for delete
  val: unknown,
  col_version: number,
  db_version: number,
  site_id: string, // hex-encoded site id
  cl: number, // causal length
  seq: number // sequence number
];

export type Changes = Array<Change>;

/**
 * Tracks whether we've received or sent changes to/from a peer
 */
export enum CrdtEvent {
  received = 0,
  sent = 1,
}

/**
 * Message types for sync protocol
 */
export enum MessageType {
  connected = "connected",
  ack = "ack",
  update = "update",
  pull = "pull",
}

/**
 * Base message payload
 */
export interface MessagePayload {
  type: MessageType;
  version: number;
  siteId: string;
  changes?: Changes;
}

/**
 * Message with channel information
 */
export interface MessageData extends MessagePayload {
  channel: string;
  lastValue?: unknown; // For tracking ack state
}

/**
 * Source of a sync event
 */
export enum Source {
  PEER = "PEER",
  TAB = "TAB",
  UI = "UI",
}

/**
 * Generic database interface that consumers must implement
 */
export interface DatabaseInterface {
  exec(sql: string, params?: unknown[]): Promise<unknown>;
  execBatch(
    statements: Array<{ sql: string; params?: unknown[] }>
  ): Promise<unknown[]>;
  transaction(fn: (tx: DatabaseInterface) => Promise<void>): Promise<void>;
  prepare?(sql: string): PreparedStatement;
}

/**
 * Prepared statement interface
 */
export interface PreparedStatement {
  run(params?: unknown[]): Promise<void>;
  get(params?: unknown[]): Promise<unknown>;
  all(params?: unknown[]): Promise<unknown[]>;
  finalize(): Promise<void>;
}

/**
 * Transport interface for sending/receiving messages
 */
export interface Transport {
  setup(handlers: TransportHandlers): void | Promise<void>;
  send(message: MessageData): void;
  isReady(): boolean;
}

/**
 * Handlers for transport events
 */
export interface TransportHandlers {
  onConnected?: (data: MessageData) => void | Promise<void>;
  onUpdate?: (data: MessageData) => void | Promise<void>;
  onAck?: (data: MessageData) => void | Promise<void>;
  onPull?: (data: MessageData) => void | Promise<void>;
}

/**
 * Repository definition for creating reactive repos
 */
export interface RepoDefinition<TView = unknown> {
  watch: string[]; // Tables to watch
  view: (db: DatabaseInterface) => Promise<TView[]>;
  commands?: {
    [key: string]: (
      db: DatabaseInterface,
      args: Record<string, unknown>
    ) => Promise<void>;
  };
}

/**
 * Command function bound to sync
 */
export type BoundCommand = (args: Record<string, unknown>) => Promise<void>;

/**
 * Commands object with bound functions
 */
export type BoundCommands = {
  [key: string]: BoundCommand;
};

/**
 * Configuration for DatabaseConnection initialization
 */
export interface DatabaseConnectionConfig {
  db: DatabaseInterface;
  name: string;
  tables: string[]; // Tables to track for CRDT
  siteId?: string; // Optional site ID, will be generated if not provided
}

/**
 * Sync event data
 */
export interface SyncEventData {
  source: Source;
  watchedTables: Set<string>;
}
