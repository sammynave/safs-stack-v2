/**
 * UI syncer handles reactive repositories and local state management
 * Framework-agnostic - consumers can wrap with their own reactivity
 */

import type { DatabaseConnection } from "../database-connection.js";
import type {
  DatabaseInterface,
  RepoDefinition,
  BoundCommands,
} from "../types.js";
import { Source } from "../types.js";
import { nanoid } from "nanoid";

/**
 * ReactiveRepo represents a reactive view of data with associated commands
 * Framework-agnostic - consumers should wrap with their own reactivity layer
 */
export class ReactiveRepo<TView = unknown> {
  id: string;
  view: TView[] = [];
  commands?: BoundCommands;

  constructor({
    id,
    view,
    commands = {},
  }: {
    id: string;
    view: TView[];
    commands?: BoundCommands;
  }) {
    this.id = id;
    this.view = view;
    this.commands = commands;
    // Spread commands onto this instance for convenient access
    Object.assign(this, commands);
  }
}

/**
 * Ui manages reactive repositories and coordinates with sync layers
 */
export class Ui {
  dbConn: DatabaseConnection;
  watchedTables: Set<string> = new Set();
  refreshFns: Map<
    string,
    { fn: () => void | Promise<void>; watchedTables: Set<string> }
  > = new Map();
  repos = new WeakMap<RepoDefinition, ReactiveRepo>();
  updateQueue = new Set<string>();
  queueing: Promise<void> | null = null;
  sync: (opts: {
    source: Source;
    watchedTables: Set<string>;
  }) => Promise<void> = async () => {
    // Will be set by Syncer
  };

  constructor({ dbConn }: { dbConn: DatabaseConnection }) {
    this.dbConn = dbConn;
  }

  /**
   * Create a reactive repository from a definition
   * Reuses existing repo if the same definition is provided
   */
  repoFor<TView>(definition: RepoDefinition<TView>): ReactiveRepo<TView> {
    if (this.repos.has(definition)) {
      return this.repos.get(definition)! as ReactiveRepo<TView>;
    }

    const { watch, view, commands } = definition;

    // Add watched tables to the connection
    watch.forEach((table) => this.watchedTables.add(table));

    const repoId = nanoid();
    const boundCommands = commands
      ? this.bindCommands(commands, new Set(watch))
      : undefined;

    const repo = new ReactiveRepo<TView>({
      id: repoId,
      view: [],
      commands: boundCommands,
    });

    // Initial view load
    view(this.dbConn.db).then((result) => {
      repo.view = result;
    });

    // Register refresh function
    this.onUpdate({
      id: repo.id,
      fn: async () => {
        repo.view = await view(this.dbConn.db);
      },
      watchedTables: new Set(watch),
    });

    this.repos.set(definition, repo);
    return repo;
  }

  /**
   * Register a refresh function for specific tables
   */
  onUpdate({
    id,
    fn,
    watchedTables,
  }: {
    id: string;
    fn: () => void | Promise<void>;
    watchedTables: Set<string>;
  }): void {
    this.refreshFns.set(id, { fn, watchedTables });
  }

  /**
   * Update all repos watching the specified tables
   * Batches updates using microtask queue
   */
  async update({
    watchedTables,
  }: {
    watchedTables: Set<string>;
  }): Promise<void> {
    // Queue repos that need updating
    for (const [id, { watchedTables: repoTables }] of this.refreshFns) {
      if ([...repoTables].some((table) => watchedTables.has(table))) {
        this.updateQueue.add(id);
      }
    }

    // Batch updates
    if (this.queueing) {
      return this.queueing;
    }

    return (this.queueing = new Promise((resolve) =>
      queueMicrotask(async () => {
        for (const id of this.updateQueue) {
          const entry = this.refreshFns.get(id);
          if (entry) {
            this.updateQueue.delete(id);
            await entry.fn();
          }
        }
        this.queueing = null;
        resolve();
      })
    ));
  }

  /**
   * Bind commands to automatically trigger syncing
   * Commands will:
   * 1. Execute the database operation
   * 2. Update all relevant repos
   * 3. Trigger sync to other tabs and peers
   */
  private bindCommands(
    commands: {
      [key: string]: (
        db: DatabaseInterface,
        args: Record<string, unknown>
      ) => Promise<void>;
    },
    watch: Set<string>
  ): BoundCommands {
    return Object.fromEntries(
      Object.entries(commands).map(([name, fn]) => [
        name,
        async (args: Record<string, unknown>) => {
          // Execute the command
          await fn(this.dbConn.db, args);

          // Update all repos watching these tables
          await this.update({ watchedTables: watch });

          // Trigger sync to other layers
          await this.sync({ source: Source.UI, watchedTables: watch });
        },
      ])
    );
  }
}
