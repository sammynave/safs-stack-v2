/**
 * CRDT trigger generation for todos table
 * These triggers track INSERT, UPDATE, and DELETE operations
 * and record them in the crdt_changes table for synchronization
 */

export function generateTodosTriggers() {
	// Track INSERT operations
	const insertTrigger = `
    CREATE TRIGGER IF NOT EXISTS crdt_track_todos_insert
    AFTER INSERT ON todos
    BEGIN
      UPDATE crdt_db_version SET version = version + 1;

      INSERT INTO crdt_changes ("table", "pk", "cid", "val", "col_version", "db_version", "site_id", "cl", "seq")
      VALUES
        ('todos', NEW.id, 'id', NEW.id, 1, (SELECT version FROM crdt_db_version), (SELECT id FROM crdt_site_id), 1, 0),
        ('todos', NEW.id, 'text', NEW.text, 1, (SELECT version FROM crdt_db_version), (SELECT id FROM crdt_site_id), 1, 0),
        ('todos', NEW.id, 'completed', NEW.completed, 1, (SELECT version FROM crdt_db_version), (SELECT id FROM crdt_site_id), 1, 0);
    END;
  `;

	// Track UPDATE operations (only for changed columns)
	const updateTrigger = `
    CREATE TRIGGER IF NOT EXISTS crdt_track_todos_update
    AFTER UPDATE ON todos
    BEGIN
      UPDATE crdt_db_version SET version = version + 1;

      INSERT INTO crdt_changes ("table", "pk", "cid", "val", "col_version", "db_version", "site_id", "cl", "seq")
      SELECT
        'todos', NEW.id, 'text', NEW.text,
        COALESCE((SELECT col_version + 1 FROM crdt_changes
                  WHERE "table" = 'todos' AND pk = NEW.id AND cid = 'text'
                  ORDER BY col_version DESC LIMIT 1), 1),
        (SELECT version FROM crdt_db_version),
        (SELECT id FROM crdt_site_id), 1, 0
      WHERE NEW.text != OLD.text

      UNION ALL

      SELECT
        'todos', NEW.id, 'completed', NEW.completed,
        COALESCE((SELECT col_version + 1 FROM crdt_changes
                  WHERE "table" = 'todos' AND pk = NEW.id AND cid = 'completed'
                  ORDER BY col_version DESC LIMIT 1), 1),
        (SELECT version FROM crdt_db_version),
        (SELECT id FROM crdt_site_id), 1, 0
      WHERE NEW.completed != OLD.completed;
    END;
  `;

	// Track DELETE operations (tombstone with cid = '-1')
	const deleteTrigger = `
    CREATE TRIGGER IF NOT EXISTS crdt_track_todos_delete
    AFTER DELETE ON todos
    BEGIN
      UPDATE crdt_db_version SET version = version + 1;

      INSERT INTO crdt_changes ("table", "pk", "cid", "val", "col_version", "db_version", "site_id", "cl", "seq")
      VALUES ('todos', OLD.id, '-1', NULL, 1,
        (SELECT version FROM crdt_db_version),
        (SELECT id FROM crdt_site_id), 1, 0);
    END;
  `;

	return { insertTrigger, updateTrigger, deleteTrigger };
}
