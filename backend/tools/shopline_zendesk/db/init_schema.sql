CREATE SCHEMA IF NOT EXISTS shopline_zendesk;

-- Shopline stores: one row per installed store
CREATE TABLE IF NOT EXISTS shopline_zendesk.stores (
  id           UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
  handle       TEXT        UNIQUE NOT NULL,
  access_token TEXT        NOT NULL,
  expires_at   TIMESTAMPTZ NOT NULL,
  scopes       TEXT,
  installed_at TIMESTAMPTZ DEFAULT NOW(),
  updated_at   TIMESTAMPTZ DEFAULT NOW()
);

-- Store-Zendesk bindings: one row per store (one-to-one)
CREATE TABLE IF NOT EXISTS shopline_zendesk.bindings (
  id                UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
  store_id          UUID        NOT NULL REFERENCES shopline_zendesk.stores(id),
  zendesk_subdomain TEXT        UNIQUE NOT NULL,
  api_key           TEXT        NOT NULL,
  created_at        TIMESTAMPTZ DEFAULT NOW(),
  updated_at        TIMESTAMPTZ DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS bindings_store_id_idx
  ON shopline_zendesk.bindings(store_id);
