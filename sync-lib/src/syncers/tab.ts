/**
 * Tab syncer handles cross-tab/window synchronization using BroadcastChannel
 */

import type { DatabaseConnection } from "../database-connection.js";
import { serializeSet, deserializeSet } from "../utils.js";

export class Tab {
  channel: BroadcastChannel | null;

  constructor({ dbConn }: { dbConn: DatabaseConnection }) {
    this.channel =
      "BroadcastChannel" in globalThis
        ? new globalThis.BroadcastChannel(dbConn.name)
        : null;
  }

  /**
   * Register a callback to handle updates from other tabs
   */
  onUpdate(fn: (data: { watchedTables: Set<string> }) => void): void {
    this.channel?.addEventListener("message", ({ data }) => {
      // Deserialize the watchedTables array back to a Set
      fn({
        watchedTables: deserializeSet(data.watchedTables),
      });
    });
  }

  /**
   * Send an update notification to other tabs
   */
  update(message: { watchedTables: Set<string> }): void {
    // Serialize the Set to an array for BroadcastChannel
    this.channel?.postMessage({
      watchedTables: serializeSet(message.watchedTables),
    });
  }

  /**
   * Close the broadcast channel
   */
  close(): void {
    this.channel?.close();
  }
}
