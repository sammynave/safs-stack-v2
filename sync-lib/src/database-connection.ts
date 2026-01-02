/**
 * DatabaseConnection wraps a vanilla SQLite database and adds CRDT tracking
 */

import type {
  DatabaseInterface,
  DatabaseConnectionConfig,
  Change,
  Changes,
} from "./types.js";
import { CrdtEvent } from "./types.js";
import { generateSiteId } from "./utils.js";
import {
  SELECT_VERSION,
  SELECT_SITE_ID,
  SELECT_CHANGES_SINCE,
  SELECT_LAST_TRACKED_VERSION_FOR_PEER,
  INSERT_CHANGES,
  UPSERT_TRACKED_PEER,
  INSERT_SITE_ID,
} from "./queries.js";
import { INIT_STATEMENTS } from "./schema.js";

export class DatabaseConnection {
  static async init(
    config: DatabaseConnectionConfig
  ): Promise<DatabaseConnection> {
    const { db, name, tables, siteId: providedSiteId } = config;

    // Initialize CRDT infrastructure
    for (const statement of INIT_STATEMENTS) {
      await db.exec(statement);
    }

    // Get or create site ID
    let siteId = providedSiteId;
    if (!siteId) {
      const result = (await db.exec(SELECT_SITE_ID)) as Array<{
        siteId: string;
      }>;
      if (result.length === 0) {
        siteId = generateSiteId();
        await db.exec(INSERT_SITE_ID, [siteId]);
      } else {
        siteId = result[0].siteId;
      }
    }

    // TODO: Generate triggers for tracked tables
    // This is complex and would require schema introspection
    // For now, users will need to manually add triggers or
    // we can provide a helper method

    return new DatabaseConnection({
      db,
      name,
      siteId: siteId!,
      tables,
    });
  }

  db: DatabaseInterface;
  name: string;
  siteId: string;
  tables: string[];

  private constructor(config: {
    db: DatabaseInterface;
    name: string;
    siteId: string;
    tables: string[];
  }) {
    this.db = config.db;
    this.name = config.name;
    this.siteId = config.siteId;
    this.tables = config.tables;
  }

  /**
   * Get the current database version
   */
  async getVersion(): Promise<number> {
    const result = (await this.db.exec(SELECT_VERSION)) as Array<{
      version: number;
    }>;
    return result[0]?.version ?? 0;
  }

  /**
   * Get the last tracked version for a specific peer
   */
  async lastTrackedVersionFor(
    siteId: string,
    event: CrdtEvent
  ): Promise<number> {
    const result = (await this.db.exec(SELECT_LAST_TRACKED_VERSION_FOR_PEER, [
      siteId,
      event,
    ])) as Array<{ version: number }>;
    return result[0]?.version ?? 0;
  }

  /**
   * Get changes since a specific version
   */
  async changesSince(since = 0): Promise<Changes> {
    const result = (await this.db.exec(SELECT_CHANGES_SINCE, [since])) as Array<
      Record<string, unknown>
    >;
    return result.map((row) => [
      row.table as string,
      row.pk as string,
      row.cid as string,
      row.val,
      row.col_version as number,
      row.db_version as number,
      row.site_id as string,
      row.cl as number,
      row.seq as number,
    ]);
  }

  /**
   * Insert a tracked peer version
   */
  async insertTrackedPeer(
    siteId: string,
    version: number,
    event: CrdtEvent
  ): Promise<void> {
    await this.db.exec(UPSERT_TRACKED_PEER, [siteId, version, event]);
  }

  /**
   * Merge incoming changes from a peer
   * Uses Last-Write-Wins (LWW) conflict resolution based on col_version
   */
  async merge(changes: Changes): Promise<void> {
    // Group changes by table for batch processing
    const changesByTable = new Map<string, Changes>();
    for (const change of changes) {
      const table = change[0];
      if (!changesByTable.has(table)) {
        changesByTable.set(table, []);
      }
      changesByTable.get(table)!.push(change);
    }

    // Process each table's changes
    for (const [table, tableChanges] of changesByTable) {
      await this._mergeTableChanges(table, tableChanges);
    }
  }

  /**
   * Merge changes for a specific table
   */
  private async _mergeTableChanges(
    table: string,
    changes: Changes
  ): Promise<void> {
    for (const change of changes) {
      const [
        tableName,
        pk,
        cid,
        val,
        col_version,
        db_version,
        site_id,
        cl,
        seq,
      ] = change;

      // Check if we already have this change or a newer version
      const existing = (await this.db.exec(
        `SELECT col_version FROM crdt_changes
         WHERE "table" = ? AND "pk" = ? AND "cid" = ?
         ORDER BY col_version DESC LIMIT 1`,
        [tableName, pk, cid]
      )) as Array<{ col_version: number }>;

      const existingVersion = existing[0]?.col_version ?? 0;

      // LWW: Only apply if this change is newer
      if (col_version > existingVersion) {
        // Insert the change record
        await this.db.exec(INSERT_CHANGES, [
          tableName,
          pk,
          cid,
          val,
          col_version,
          db_version,
          site_id,
          cl,
          seq,
        ]);

        // Apply the change to the actual table
        await this._applyChange(change);
      }
    }
  }

  /**
   * Apply a change to the actual data table
   * This reconstructs the data from change records
   */
  private async _applyChange(change: Change): Promise<void> {
    const [tableName, pk, cid, val] = change;

    // Handle deletions (cid = -1)
    if (cid === "-1") {
      // TODO: Implement deletion
      // Need to parse pk and construct DELETE statement
      return;
    }

    // For updates/inserts, we need to:
    // 1. Check if row exists
    // 2. If exists, UPDATE the column
    // 3. If not exists, INSERT the row (need all columns from changes)

    // This is simplified - production code would need more sophisticated logic
    try {
      await this.db.exec(
        `INSERT INTO ${tableName} (${cid}) VALUES (?)
         ON CONFLICT DO UPDATE SET ${cid} = excluded.${cid}`,
        [val]
      );
    } catch (error) {
      console.warn(`Failed to apply change to ${tableName}:`, error);
    }
  }

  /**
   * Bulk load changes (optimized for initial sync)
   * Bypasses triggers and directly inserts into crdt_changes
   */
  async bulkLoad(changes: Changes): Promise<void> {
    const statements = changes.map((change) => ({
      sql: INSERT_CHANGES,
      params: change,
    }));

    await this.db.execBatch(statements);

    // After bulk loading, reconstruct the data tables
    await this._reconstructFromChanges();
  }

  /**
   * Reconstruct data tables from change records
   * Used after bulk loading changes
   */
  private async _reconstructFromChanges(): Promise<void> {
    // Group changes by table and pk, keeping only the latest col_version for each cid
    for (const table of this.tables) {
      const changes = (await this.db.exec(
        `SELECT "pk", "cid", "val", MAX("col_version") as col_version
         FROM crdt_changes
         WHERE "table" = ? AND "cid" != '-1'
         GROUP BY "pk", "cid"`,
        [table]
      )) as Array<{ pk: string; cid: string; val: unknown }>;

      // Group by pk to construct full rows
      const rowsByPk = new Map<string, Map<string, unknown>>();
      for (const { pk, cid, val } of changes) {
        if (!rowsByPk.has(pk)) {
          rowsByPk.set(pk, new Map());
        }
        rowsByPk.get(pk)!.set(cid, val);
      }

      // Insert/update rows
      for (const [pk, columns] of rowsByPk) {
        const columnNames = Array.from(columns.keys());
        const values = Array.from(columns.values());

        const placeholders = columnNames.map(() => "?").join(", ");
        const updates = columnNames
          .map((col) => `${col} = excluded.${col}`)
          .join(", ");

        try {
          await this.db.exec(
            `INSERT INTO ${table} (${columnNames.join(", ")})
             VALUES (${placeholders})
             ON CONFLICT DO UPDATE SET ${updates}`,
            values
          );
        } catch (error) {
          console.warn(`Failed to reconstruct row in ${table}:`, error);
        }
      }
    }
  }
}
