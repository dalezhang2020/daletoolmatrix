-- OAuth migration: Add oauth_states table and token refresh tracking columns.
--
-- New table stores short-lived OAuth state parameters for CSRF protection
-- during the Shopline OAuth 2.0 authorization flow.
--
-- New columns on stores track consecutive token refresh failures so the
-- refresh job can mark a token as invalid after 3 failures.
--
-- Idempotent: safe to run multiple times.

-- OAuth state table for CSRF protection
CREATE TABLE IF NOT EXISTS shopline_zendesk.oauth_states (
  id                UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
  state             TEXT        UNIQUE NOT NULL,
  zendesk_subdomain TEXT        NOT NULL,
  handle            TEXT        NOT NULL,
  created_at        TIMESTAMPTZ DEFAULT NOW(),
  expires_at        TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS oauth_states_expires_idx
  ON shopline_zendesk.oauth_states(expires_at);

-- Token refresh failure tracking on stores
ALTER TABLE shopline_zendesk.stores
  ADD COLUMN IF NOT EXISTS refresh_fail_count INTEGER DEFAULT 0;

ALTER TABLE shopline_zendesk.stores
  ADD COLUMN IF NOT EXISTS token_invalid BOOLEAN DEFAULT FALSE;
