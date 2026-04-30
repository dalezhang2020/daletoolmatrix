-- Phase 2 migration: Add Zendesk credentials to bindings table.
--
-- Original columns (zendesk_admin_email, zendesk_api_token) are kept
-- for backward compatibility with existing prod data.
--
-- New columns store OAuth 2.0 tokens obtained via the Zendesk
-- Authorization Code flow.
--
-- Idempotent: safe to run multiple times via ADD COLUMN IF NOT EXISTS.

-- Legacy columns (kept for backward compat — do NOT drop)
ALTER TABLE shopline_zendesk.bindings
  ADD COLUMN IF NOT EXISTS zendesk_admin_email TEXT;

ALTER TABLE shopline_zendesk.bindings
  ADD COLUMN IF NOT EXISTS zendesk_api_token TEXT;

-- OAuth 2.0 token columns
ALTER TABLE shopline_zendesk.bindings
  ADD COLUMN IF NOT EXISTS zendesk_access_token TEXT;

ALTER TABLE shopline_zendesk.bindings
  ADD COLUMN IF NOT EXISTS zendesk_refresh_token TEXT;

ALTER TABLE shopline_zendesk.bindings
  ADD COLUMN IF NOT EXISTS zendesk_token_expires_at TIMESTAMPTZ;
