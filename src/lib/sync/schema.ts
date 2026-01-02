/**
 * Schema definitions for CRDT tracking tables
 * These replicate CR-SQLite's functionality using vanilla SQLite
 */

/**
 * Create the site_id table to store this node's unique identifier
 */
export const CREATE_SITE_ID_TABLE = `
CREATE TABLE IF NOT EXISTS crdt_site_id (
  id TEXT PRIMARY KEY
);
`;

/**
 * Create the db_version table to track global database version
 */
export const CREATE_DB_VERSION_TABLE = `
CREATE TABLE IF NOT EXISTS crdt_db_version (
  version INTEGER PRIMARY KEY DEFAULT 0
);
`;

/**
 * Initialize db_version to 0
 */
export const INIT_DB_VERSION = `
INSERT INTO crdt_db_version (version)
SELECT 0
WHERE NOT EXISTS (SELECT 1 FROM crdt_db_version);
`;

/**
 * Create the changes table to track all modifications
 * This replicates crsql_changes from CR-SQLite
 */
export const CREATE_CHANGES_TABLE = `
CREATE TABLE IF NOT EXISTS crdt_changes (
  "table" TEXT NOT NULL,
  "pk" TEXT NOT NULL,
  "cid" TEXT NOT NULL,
  "val" TEXT,
  "col_version" INTEGER NOT NULL,
  "db_version" INTEGER NOT NULL,
  "site_id" TEXT NOT NULL,
  "cl" INTEGER NOT NULL DEFAULT 1,
  "seq" INTEGER NOT NULL DEFAULT 0,
  PRIMARY KEY ("table", "pk", "cid", "db_version", "site_id")
) WITHOUT ROWID;
`;

/**
 * Index for efficient querying of changes since a version
 */
export const CREATE_CHANGES_INDEX = `
CREATE INDEX IF NOT EXISTS idx_crdt_changes_db_version
ON crdt_changes(db_version);
`;

/**
 * Index for querying changes by site
 */
export const CREATE_CHANGES_SITE_INDEX = `
CREATE INDEX IF NOT EXISTS idx_crdt_changes_site_id
ON crdt_changes(site_id, db_version);
`;

/**
 * Create the tracked_peers table to track sync state with other nodes
 * This replicates crsql_tracked_peers from CR-SQLite
 */
export const CREATE_TRACKED_PEERS_TABLE = `
CREATE TABLE IF NOT EXISTS crdt_tracked_peers (
  "site_id" TEXT NOT NULL,
  "version" INTEGER NOT NULL DEFAULT 0,
  "tag" INTEGER NOT NULL DEFAULT 0,
  "event" INTEGER NOT NULL,
  PRIMARY KEY ("site_id", "tag", "event")
) WITHOUT ROWID;
`;

/**
 * Generate trigger SQL for a specific table to track INSERT operations
 */
export function generateInsertTrigger(tableName: string): string {
  return `
CREATE TRIGGER IF NOT EXISTS crdt_track_${tableName}_insert
AFTER INSERT ON ${tableName}
BEGIN
  -- Increment db version
  UPDATE crdt_db_version SET version = version + 1;

  -- Insert change records for each non-primary-key column
  -- This is a simplified version - you may need to customize based on your schema
  INSERT INTO crdt_changes ("table", "pk", "cid", "val", "col_version", "db_version", "site_id", "cl", "seq")
  SELECT
    '${tableName}',
    (SELECT json_group_array(value) FROM json_each(json_object(${generatePkColumns(
      tableName
    )}))),
    key,
    value,
    1, -- initial col_version
    (SELECT version FROM crdt_db_version),
    (SELECT id FROM crdt_site_id),
    1,
    0
  FROM json_each(json_object(${generateAllColumns(tableName)}));
END;
`;
}

/**
 * Generate trigger SQL for a specific table to track UPDATE operations
 */
export function generateUpdateTrigger(tableName: string): string {
  return `
CREATE TRIGGER IF NOT EXISTS crdt_track_${tableName}_update
AFTER UPDATE ON ${tableName}
BEGIN
  -- Increment db version
  UPDATE crdt_db_version SET version = version + 1;

  -- Insert change records for changed columns
  INSERT INTO crdt_changes ("table", "pk", "cid", "val", "col_version", "db_version", "site_id", "cl", "seq")
  SELECT
    '${tableName}',
    (SELECT json_group_array(value) FROM json_each(json_object(${generatePkColumns(
      tableName
    )}))),
    key,
    value,
    COALESCE(
      (SELECT col_version + 1
       FROM crdt_changes
       WHERE "table" = '${tableName}'
         AND "pk" = (SELECT json_group_array(value) FROM json_each(json_object(${generatePkColumns(
           tableName
         )})))
         AND "cid" = key
       ORDER BY col_version DESC
       LIMIT 1),
      1
    ),
    (SELECT version FROM crdt_db_version),
    (SELECT id FROM crdt_site_id),
    1,
    0
  FROM json_each(json_object(${generateAllColumns(tableName)}))
  WHERE value != (
    SELECT json_extract(json_object(${generateOldColumns(
      tableName
    )}), '$.' || key)
  );
END;
`;
}

/**
 * Generate trigger SQL for a specific table to track DELETE operations
 */
export function generateDeleteTrigger(tableName: string): string {
  return `
CREATE TRIGGER IF NOT EXISTS crdt_track_${tableName}_delete
AFTER DELETE ON ${tableName}
BEGIN
  -- Increment db version
  UPDATE crdt_db_version SET version = version + 1;

  -- Insert a tombstone record (cid = -1 indicates deletion)
  INSERT INTO crdt_changes ("table", "pk", "cid", "val", "col_version", "db_version", "site_id", "cl", "seq")
  VALUES (
    '${tableName}',
    (SELECT json_group_array(value) FROM json_each(json_object(${generateOldPkColumns(
      tableName
    )}))),
    '-1',
    NULL,
    1,
    (SELECT version FROM crdt_db_version),
    (SELECT id FROM crdt_site_id),
    1,
    0
  );
END;
`;
}

/**
 * Helper function to generate primary key column references
 * This is a placeholder - actual implementation should introspect schema
 */
function generatePkColumns(tableName: string): string {
  // This should be customized based on your schema
  // For now, assume 'id' is the primary key
  return "'id', NEW.id";
}

/**
 * Helper function to generate OLD primary key column references
 */
function generateOldPkColumns(tableName: string): string {
  return "'id', OLD.id";
}

/**
 * Helper function to generate all column references for NEW
 */
function generateAllColumns(tableName: string): string {
  // This should be customized based on your schema
  // This is a placeholder that will need to be replaced with actual column introspection
  return "/* columns here */";
}

/**
 * Helper function to generate all column references for OLD
 */
function generateOldColumns(tableName: string): string {
  // This should be customized based on your schema
  return "/* old columns here */";
}

/**
 * Initialize CRDT tracking infrastructure
 */
export const INIT_STATEMENTS = [
  CREATE_SITE_ID_TABLE,
  CREATE_DB_VERSION_TABLE,
  INIT_DB_VERSION,
  CREATE_CHANGES_TABLE,
  CREATE_CHANGES_INDEX,
  CREATE_CHANGES_SITE_INDEX,
  CREATE_TRACKED_PEERS_TABLE,
];

/**
 * Note: Trigger generation is simplified here. In practice, you'll need to:
 * 1. Introspect the table schema to get column names and primary keys
 * 2. Generate appropriate JSON for multi-column primary keys
 * 3. Handle different column types appropriately
 *
 * Consider using a schema introspection library or requiring users to
 * provide column definitions explicitly.
 */
