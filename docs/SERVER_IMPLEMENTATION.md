# Server Implementation Guide

This document describes how to implement the server (NODE B) for CRDT synchronization.

## Overview

The server acts as a central relay for CRDT changes between clients. It:
1. Maintains a SQLite database per user/channel
2. Receives changes from clients
3. Persists changes to its database
4. Forwards changes to all connected peers
5. Tracks peer versions and sends acknowledgments

## Architecture

```
┌─────────────────────────────────────────────────────┐
│                   WebSocket Server                   │
├─────────────────────────────────────────────────────┤
│  ┌───────────────┐  ┌───────────────┐  ┌──────────┐│
│  │  Connection   │  │  Connection   │  │   Per-   ││
│  │   Handler     │  │   Handler     │  │  Channel ││
│  │  (Client A)   │  │  (Client B)   │  │ Database ││
│  └───────────────┘  └───────────────┘  └──────────┘│
└─────────────────────────────────────────────────────┘
```

## Message Flow

### 1. Client Connection

When a client connects:

```typescript
// Server receives WebSocket connection
ws.on('connection', async (socket) => {
  // Generate or retrieve server site ID
  const serverSiteId = await getOrCreateSiteId();

  // Get current database version
  const version = await dbConn.getVersion();

  // Send connected message to client
  socket.send(JSON.stringify({
    type: MessageType.connected,
    siteId: serverSiteId,
    version: version,
    channel: 'user-channel-name'
  }));
});
```

### 2. Receiving Updates from Client

```typescript
socket.on('message', async (data) => {
  const message = JSON.parse(data);

  if (message.type === MessageType.update) {
    const { changes, version, siteId, channel } = message;

    // Get the appropriate database for this channel
    const dbConn = await getChannelDatabase(channel);

    // Merge changes into server database
    await dbConn.merge(changes);

    // Update peer tracking table
    await dbConn.insertTrackedPeer(siteId, version, CrdtEvent.received);

    // Send ack to sender
    socket.send(JSON.stringify({
      type: MessageType.ack,
      siteId: serverSiteId,
      version: await dbConn.getVersion(),
      channel
    }));

    // Forward changes to all other connected clients in this channel
    broadcastToChannel(channel, {
      type: MessageType.update,
      changes,
      version: await dbConn.getVersion(),
      siteId: serverSiteId,
      channel
    }, socket); // Exclude the sender
  }
});
```

### 3. Handling Pull Requests

```typescript
if (message.type === MessageType.pull) {
  const { version, siteId, channel } = message;

  // Get the appropriate database
  const dbConn = await getChannelDatabase(channel);

  // Get all changes since the requested version
  const changes = await dbConn.changesSince(version);

  // Send changes to client
  socket.send(JSON.stringify({
    type: MessageType.update,
    changes,
    version: await dbConn.getVersion(),
    siteId: serverSiteId,
    channel
  }));
}
```

## Example Implementation (Node.js + ws)

```typescript
import { WebSocketServer } from 'ws';
import { DatabaseConnection } from './database-connection';
import { SqliteAdapter } from './sqlite-adapter';
import Database from 'better-sqlite3';

// Store database connections per channel
const channelDatabases = new Map<string, DatabaseConnection>();
const channelConnections = new Map<string, Set<WebSocket>>();

async function getChannelDatabase(channel: string): Promise<DatabaseConnection> {
  if (!channelDatabases.has(channel)) {
    // Create new database for this channel
    const sqlite = new Database(`./data/${channel}.db`);
    const adapter = new SqliteAdapter(sqlite);

    const dbConn = await DatabaseConnection.init({
      db: adapter,
      name: channel,
      tables: ['todos'] // Configure based on your schema
    });

    channelDatabases.set(channel, dbConn);
  }

  return channelDatabases.get(channel)!;
}

const wss = new WebSocketServer({ port: 8080 });

wss.on('connection', async (socket, request) => {
  // Extract channel from URL or auth token
  const channel = extractChannel(request);

  // Add socket to channel connections
  if (!channelConnections.has(channel)) {
    channelConnections.set(channel, new Set());
  }
  channelConnections.get(channel)!.add(socket);

  const dbConn = await getChannelDatabase(channel);
  const serverSiteId = dbConn.siteId;
  const version = await dbConn.getVersion();

  // Send connected message
  socket.send(JSON.stringify({
    type: 'connected',
    siteId: serverSiteId,
    version: version,
    channel
  }));

  socket.on('message', async (data) => {
    try {
      const message = JSON.parse(data.toString());

      switch (message.type) {
        case 'update':
          await handleUpdate(socket, message, channel, dbConn);
          break;
        case 'pull':
          await handlePull(socket, message, channel, dbConn);
          break;
      }
    } catch (error) {
      console.error('Error handling message:', error);
    }
  });

  socket.on('close', () => {
    channelConnections.get(channel)?.delete(socket);
  });
});

async function handleUpdate(
  socket: WebSocket,
  message: any,
  channel: string,
  dbConn: DatabaseConnection
) {
  const { changes, version, siteId } = message;

  // Merge changes
  await dbConn.merge(changes);

  // Track that we received these changes
  await dbConn.insertTrackedPeer(siteId, version, CrdtEvent.received);

  // Send ack
  socket.send(JSON.stringify({
    type: 'ack',
    siteId: dbConn.siteId,
    version: await dbConn.getVersion(),
    channel
  }));

  // Broadcast to other clients
  const serverVersion = await dbConn.getVersion();
  broadcastToChannel(channel, {
    type: 'update',
    changes,
    version: serverVersion,
    siteId: dbConn.siteId,
    channel
  }, socket);
}

async function handlePull(
  socket: WebSocket,
  message: any,
  channel: string,
  dbConn: DatabaseConnection
) {
  const { version } = message;

  const changes = await dbConn.changesSince(version);

  socket.send(JSON.stringify({
    type: 'update',
    changes,
    version: await dbConn.getVersion(),
    siteId: dbConn.siteId,
    channel
  }));
}

function broadcastToChannel(
  channel: string,
  message: any,
  excludeSocket?: WebSocket
) {
  const sockets = channelConnections.get(channel);
  if (!sockets) return;

  const data = JSON.stringify(message);
  for (const socket of sockets) {
    if (socket !== excludeSocket && socket.readyState === WebSocket.OPEN) {
      socket.send(data);
    }
  }
}

function extractChannel(request: any): string {
  // Extract from URL query, auth token, etc.
  const url = new URL(request.url, 'http://localhost');
  return url.searchParams.get('channel') || 'default';
}
```

## Multi-Server Deployment

For horizontal scaling across multiple servers, use Redis Pub/Sub:

```typescript
import Redis from 'ioredis';

const redis = new Redis();
const pub = new Redis();

// Subscribe to channel updates
redis.subscribe('crdt-updates');

redis.on('message', (channel, message) => {
  if (channel === 'crdt-updates') {
    const update = JSON.parse(message);

    // Broadcast to local WebSocket connections
    broadcastToChannel(update.channel, update.message);
  }
});

// When receiving updates from clients, publish to Redis
async function handleUpdate(socket, message, channel, dbConn) {
  // ... persist to database ...

  // Publish to Redis for other servers
  pub.publish('crdt-updates', JSON.stringify({
    channel,
    message: {
      type: 'update',
      changes,
      version: serverVersion,
      siteId: dbConn.siteId,
      channel
    }
  }));
}
```

## Database Schema

The server uses the same CRDT tables as clients:

- `crdt_site_id` - Server's unique site ID
- `crdt_db_version` - Global version counter
- `crdt_changes` - All CRDT changes
- `crdt_tracked_peers` - Track sync state with clients

Plus your application tables (e.g., `todos`).

## Security Considerations

1. **Authentication**: Verify user identity before allowing connections
2. **Authorization**: Ensure users can only access their own channels
3. **Rate Limiting**: Prevent abuse of the sync endpoint
4. **Validation**: Validate incoming changes before persisting
5. **Encryption**: Use WSS (WebSocket Secure) in production

## Testing

Test the server with:

```bash
# Start server
node server.js

# Connect with test client
wscat -c ws://localhost:8080?channel=test-channel
```

## Monitoring

Track these metrics:
- Number of active connections per channel
- Message throughput (messages/second)
- Database version lag between server and clients
- Error rates for merge operations

## Example Project

See the `habits` project for a complete reference implementation:
https://github.com/sammynave/habits
