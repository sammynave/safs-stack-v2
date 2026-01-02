/**
 * SQL queries for CRDT change tracking
 * These replicate CR-SQLite query patterns using vanilla SQLite
 */

/**
 * Get the current database version
 */
export const SELECT_VERSION = `
SELECT version FROM crdt_db_version;
`;

/**
 * Get this node's site ID
 */
export const SELECT_SITE_ID = `
SELECT id as siteId FROM crdt_site_id;
`;

/**
 * Get all changes
 */
export const SELECT_ALL_CHANGES = `
SELECT "table", "pk", "cid", "val",
  "col_version", "db_version", "site_id", "cl", "seq"
FROM crdt_changes
ORDER BY db_version DESC;
`;

/**
 * Get changes since a specific database version
 */
export const SELECT_CHANGES_SINCE = `
SELECT "table", "pk", "cid", "val",
  "col_version", "db_version", "site_id", "cl", "seq"
FROM crdt_changes
WHERE db_version > ?;
`;

/**
 * Get changes from this client since a specific version
 */
export const SELECT_CLIENT_CHANGES_SINCE = `
SELECT "table", "pk", "cid", "val",
  "col_version", "db_version", "site_id", "cl", "seq"
FROM crdt_changes
WHERE db_version > ? AND site_id = ?;
`;

/**
 * Get the last tracked version for a peer
 */
export const SELECT_LAST_TRACKED_VERSION_FOR_PEER = `
SELECT IFNULL(version, 0) as version
FROM crdt_tracked_peers
WHERE site_id = ? AND event = ?;
`;

/**
 * Get changes from peers (not from this client)
 */
export const SELECT_NON_CLIENT_CHANGES = `
SELECT "table", "pk", "cid", "val",
  "col_version", "db_version", "site_id", "cl", "seq"
FROM crdt_changes
WHERE site_id != :clientSiteId AND db_version > :dbVersion;
`;

/**
 * Get changes from peers excluding tombstones (for initial sync)
 */
export const SELECT_NON_CLIENT_NO_TOMBSTONES_CHANGES = `
SELECT "table", "pk", "cid", "val",
  "col_version", "db_version", "site_id", "cl", "seq"
FROM crdt_changes
WHERE site_id != :clientSiteId
  AND db_version > :dbVersion
  AND cid != '-1';
`;

/**
 * Insert a change record
 */
export const INSERT_CHANGES = `
INSERT INTO crdt_changes
  ("table", "pk", "cid", "val", "col_version", "db_version", "site_id", "cl", "seq")
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);
`;

/**
 * Upsert a tracked peer record
 */
export const UPSERT_TRACKED_PEER = `
INSERT INTO crdt_tracked_peers (site_id, version, tag, event)
VALUES (?, ?, 0, ?)
ON CONFLICT(site_id, tag, event)
DO UPDATE SET version = excluded.version;
`;

/**
 * Insert site ID
 */
export const INSERT_SITE_ID = `
INSERT INTO crdt_site_id (id) VALUES (?);
`;

/**
 * Get table info for introspection
 */
export const GET_TABLE_INFO = `
PRAGMA table_info(?);
`;

/**
 * Get primary key columns for a table
 */
export const GET_PRIMARY_KEY_COLUMNS = `
SELECT name FROM pragma_table_info(?) WHERE pk > 0 ORDER BY pk;
`;

/**
 * Get all columns for a table
 */
export const GET_ALL_COLUMNS = `
SELECT name FROM pragma_table_info(?);
`;
