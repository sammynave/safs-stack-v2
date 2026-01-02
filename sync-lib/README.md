# sync-lib

A framework-agnostic CRDT (Conflict-free Replicated Data Type) sync library for vanilla SQLite. Provides three-layer synchronization without requiring CR-SQLite.

## Features

- **Framework-agnostic**: Works with any JavaScript framework or vanilla JS
- **Three-layer sync architecture**:
  - **UI Layer**: Reactive repositories with automatic command binding
  - **Tab Layer**: Cross-tab/window synchronization via BroadcastChannel
  - **Peer Layer**: Remote peer synchronization via WebSocket
- **Vanilla SQLite**: Uses triggers and tracking tables instead of CR-SQLite
- **Last-Write-Wins (LWW)**: Simple conflict resolution strategy
- **TypeScript**: Fully typed API

## Architecture

```
┌─────────────────────────────────────┐
│         UI Layer (Reactive)         │  <- Reactive repos, commands
├─────────────────────────────────────┤
│       Tab Layer (BroadcastChannel)  │  <- Cross-tab sync
├─────────────────────────────────────┤
│      Peer Layer (WebSocket)         │  <- Remote peer sync
└─────────────────────────────────────┘
```

## Installation

```bash
npm install sync-lib
# or
pnpm add sync-lib
# or
yarn add sync-lib
```

## Quick Start

```typescript
import { DatabaseConnection, Syncer, WsTransport } from 'sync-lib';

// 1. Wrap your SQLite database
const dbConn = await DatabaseConnection.init({
  db: yourSQLiteInstance, // Must implement DatabaseInterface
  name: 'my-app-db',
  tables: ['users', 'posts', 'comments'] // Tables to track
});

// 2. (Optional) Set up WebSocket transport for remote sync
const ws = new WebSocket('wss://your-server.com/sync');
const transport = new WsTransport(ws);

// 3. Initialize the syncer
const syncer = await Syncer.init({
  dbConn,
  transport // optional - omit for local-only sync
});

// 4. Create reactive repositories
const usersRepo = syncer.ui.repoFor({
  watch: ['users'],
  view: async (db) => {
    const result = await db.exec('SELECT * FROM users');
    return result as User[];
  },
  commands: {
    addUser: async (db, { name, email }) => {
      await db.exec('INSERT INTO users (name, email) VALUES (?, ?)', [name, email]);
    },
    updateUser: async (db, { id, name }) => {
      await db.exec('UPDATE users SET name = ? WHERE id = ?', [name, id]);
    },
    deleteUser: async (db, { id }) => {
      await db.exec('DELETE FROM users WHERE id = ?', [id]);
    }
  }
});

// 5. Use the repo
console.log(usersRepo.view); // Current data
await usersRepo.addUser({ name: 'Alice', email: 'alice@example.com' });
// Repository automatically updates, syncs to other tabs, and syncs to remote peers!
```

## Core Concepts

### DatabaseInterface

Your SQLite implementation must provide:

```typescript
interface DatabaseInterface {
  exec(sql: string, params?: unknown[]): Promise<unknown>;
  execBatch(statements: Array<{ sql: string; params?: unknown[] }>): Promise<unknown[]>;
  transaction(fn: (tx: DatabaseInterface) => Promise<void>): Promise<void>;
}
```

### CRDT Tracking Tables

The library creates hidden tracking tables:

- `crdt_site_id`: Stores this node's unique identifier
- `crdt_db_version`: Global database version counter
- `crdt_changes`: Records all modifications (like CR-SQLite's `crsql_changes`)
- `crdt_tracked_peers`: Tracks sync state with other nodes

### Triggers (Important!)

Currently, you need to manually create triggers for tracked tables. The library provides helper functions:

```typescript
import { generateInsertTrigger, generateUpdateTrigger, generateDeleteTrigger } from 'sync-lib';

// For each table you want to track:
await db.exec(generateInsertTrigger('users'));
await db.exec(generateUpdateTrigger('users'));
await db.exec(generateDeleteTrigger('users'));
```

**Note**: Trigger generation is simplified and assumes single-column primary keys named `id`. You may need to customize for complex schemas.

### Reactive Repositories

Repositories provide a reactive view of your data:

```typescript
const repo = syncer.ui.repoFor({
  watch: ['table1', 'table2'], // Tables to watch
  view: async (db) => {
    // Query to refresh when tables change
    return await db.exec('SELECT ...');
  },
  commands: {
    // Commands automatically trigger sync
    myCommand: async (db, args) => {
      await db.exec('UPDATE ...');
    }
  }
});

// Access current view
console.log(repo.view);

// Execute commands (triggers sync automatically)
await repo.myCommand({ ... });
```

### Sync Flow

When you execute a command:
1. Database operation executes
2. Trigger records change to `crdt_changes`
3. Local repos watching affected tables refresh
4. Change broadcasts to other tabs via BroadcastChannel
5. Change pushes to remote peers via WebSocket
6. Other tabs/peers receive and merge changes
7. Their local repos refresh automatically

## Advanced Usage

### Custom Transport

Implement the `Transport` interface for custom sync mechanisms:

```typescript
class MyTransport implements Transport {
  async setup(handlers: TransportHandlers): Promise<void> {
    // Set up connection and route messages to handlers
  }

  send(message: MessageData): void {
    // Send message to peer
  }

  isReady(): boolean {
    // Check if ready to send
  }
}
```

### Manual Sync Control

```typescript
// Manually trigger sync
await syncer.sync({
  source: Source.UI,
  watchedTables: new Set(['users'])
});

// Access individual layers
syncer.ui.update({ watchedTables: new Set(['users']) });
syncer.tab.update({ watchedTables: new Set(['users']) });
await syncer.peer?.pushChangesSince({ sinceVersion: 0 });
```

### Direct Database Access

```typescript
// Access the wrapped database
const version = await syncer.dbConn.getVersion();
const changes = await syncer.dbConn.changesSince(10);
await syncer.dbConn.merge(incomingChanges);
```

## Server-Side Usage

To implement a sync server, you'll need to:

1. Create a WebSocket server
2. Maintain server-side SQLite databases (one per channel/user)
3. Relay changes between clients
4. Use Redis pub/sub for multi-server deployments (optional)

See the habits project for a complete server implementation example.

## Limitations & TODOs

- **Trigger generation**: Currently simplified - requires manual customization for complex schemas
- **Schema introspection**: Not yet implemented - you must know your table structure
- **Deletion handling**: Tombstone records are created but reconstruction is incomplete
- **Conflict resolution**: Only Last-Write-Wins - no custom strategies yet
- **Compression**: Changes are not compressed for transport
- **Batching**: Limited batching optimization

## Comparison with CR-SQLite

| Feature | CR-SQLite | sync-lib |
|---------|-----------|----------|
| Change tracking | Automatic | Manual triggers |
| SQLite version | Modified | Vanilla |
| Performance | Optimized | Good |
| Setup | Simple | More setup |
| Portability | Limited | High |
| Dependencies | Binary | None |

## Examples

See the `/examples` directory (TODO) for complete working examples.

## Contributing

This is an extracted library from a larger project. Contributions welcome!

## License

MIT

## Credits

Extracted and adapted from the sync patterns in the habits project, which was inspired by CR-SQLite and Vlcn.io.
