-- +goose Up
-- Users table for authentication
-- Stores user credentials and role assignments
CREATE TABLE auth_users (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    email TEXT NOT NULL UNIQUE,
    password_hash TEXT NOT NULL,

    -- Role: viewer (read-only), editor (CRUD), admin (full control)
    role TEXT NOT NULL DEFAULT 'viewer' CHECK (role IN ('viewer', 'editor', 'admin')),

    -- User display info
    name TEXT NOT NULL DEFAULT '',

    -- Account status
    is_active BOOLEAN NOT NULL DEFAULT true,
    email_verified BOOLEAN NOT NULL DEFAULT false,

    -- Timestamps
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_login_at TIMESTAMPTZ
);

-- Index for login lookups
CREATE INDEX idx_auth_users_email ON auth_users(email);

-- Index for active users filtering
CREATE INDEX idx_auth_users_active ON auth_users(is_active) WHERE is_active = true;

-- Trigger to update updated_at on changes
CREATE OR REPLACE FUNCTION update_auth_users_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_auth_users_updated_at
    BEFORE UPDATE ON auth_users
    FOR EACH ROW
    EXECUTE FUNCTION update_auth_users_updated_at();

-- +goose Down
DROP TRIGGER IF EXISTS trigger_auth_users_updated_at ON auth_users;
DROP FUNCTION IF EXISTS update_auth_users_updated_at;
DROP TABLE IF EXISTS auth_users;
