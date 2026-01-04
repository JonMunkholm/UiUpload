-- +goose Up
-- Sessions table for cookie-based authentication
-- Supports both short-lived (24h) and persistent (30d) sessions
CREATE TABLE auth_sessions (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id UUID NOT NULL REFERENCES auth_users(id) ON DELETE CASCADE,

    -- Session token (stored as hash for security)
    token_hash TEXT NOT NULL UNIQUE,

    -- CSRF token for this session (stored as hash)
    csrf_token_hash TEXT NOT NULL,

    -- Session metadata
    user_agent TEXT,
    ip_address INET,

    -- Expiration
    expires_at TIMESTAMPTZ NOT NULL,
    remember_me BOOLEAN NOT NULL DEFAULT false,

    -- Timestamps
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_activity_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Index for token lookups (primary access pattern)
CREATE INDEX idx_auth_sessions_token ON auth_sessions(token_hash);

-- Index for user session listing
CREATE INDEX idx_auth_sessions_user ON auth_sessions(user_id);

-- Index for cleanup of expired sessions
CREATE INDEX idx_auth_sessions_expires ON auth_sessions(expires_at);

-- Function to clean up expired sessions (call periodically)
CREATE OR REPLACE FUNCTION cleanup_expired_sessions()
RETURNS INTEGER AS $$
DECLARE
    deleted_count INTEGER;
BEGIN
    DELETE FROM auth_sessions WHERE expires_at < NOW();
    GET DIAGNOSTICS deleted_count = ROW_COUNT;
    RETURN deleted_count;
END;
$$ LANGUAGE plpgsql;

-- +goose Down
DROP FUNCTION IF EXISTS cleanup_expired_sessions;
DROP TABLE IF EXISTS auth_sessions;
