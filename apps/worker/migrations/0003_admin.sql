ALTER TABLE users ADD COLUMN admin_revision INTEGER NOT NULL DEFAULT 0;
ALTER TABLE invites ADD COLUMN revoked_at TEXT;
ALTER TABLE invites ADD COLUMN revoked_by TEXT REFERENCES users (id);
CREATE INDEX users_admin_list ON users (created_at DESC, id);
CREATE INDEX invites_admin_list ON invites (created_at DESC, code_hash);
CREATE INDEX audit_admin_list ON audit_log (created_at DESC, id);
CREATE INDEX audit_admin_action ON audit_log (action, created_at DESC, id);

-- This invariant is evaluated inside the serialized write transaction, including
-- concurrent admins disabling/demoting themselves or each other.
CREATE TRIGGER users_keep_last_admin_update
BEFORE UPDATE OF role, disabled_at ON users
WHEN OLD.role = 'admin' AND OLD.disabled_at IS NULL
  AND (NEW.role <> 'admin' OR NEW.disabled_at IS NOT NULL)
  AND NOT EXISTS (SELECT 1 FROM users WHERE id <> OLD.id AND role = 'admin' AND disabled_at IS NULL)
BEGIN
  SELECT RAISE(ABORT, 'LAST_ACTIVE_ADMIN');
END;

CREATE TRIGGER users_keep_last_admin_delete
BEFORE DELETE ON users
WHEN OLD.role = 'admin' AND OLD.disabled_at IS NULL
  AND NOT EXISTS (SELECT 1 FROM users WHERE id <> OLD.id AND role = 'admin' AND disabled_at IS NULL)
BEGIN
  SELECT RAISE(ABORT, 'LAST_ACTIVE_ADMIN');
END;
