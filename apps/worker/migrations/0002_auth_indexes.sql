CREATE INDEX sessions_expiry ON sessions (expires_at);
CREATE INDEX rate_limits_window ON rate_limits (window_start);
