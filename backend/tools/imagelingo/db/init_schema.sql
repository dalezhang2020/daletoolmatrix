CREATE SCHEMA IF NOT EXISTS imagelingo;

CREATE TABLE IF NOT EXISTS imagelingo.stores (
  id           UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  handle       TEXT UNIQUE NOT NULL,
  access_token TEXT NOT NULL,
  expires_at   TIMESTAMPTZ NOT NULL,
  scopes       TEXT,
  installed_at TIMESTAMPTZ DEFAULT NOW(),
  updated_at   TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS imagelingo.translation_jobs (
  id                 UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  store_id           UUID REFERENCES imagelingo.stores(id),
  product_id         TEXT NOT NULL,
  original_image_url TEXT NOT NULL,
  target_languages   TEXT[] NOT NULL,
  status             TEXT DEFAULT 'pending',
  error_msg          TEXT,
  created_at         TIMESTAMPTZ DEFAULT NOW(),
  completed_at       TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS imagelingo.translated_images (
  id                UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  job_id            UUID REFERENCES imagelingo.translation_jobs(id),
  language          TEXT NOT NULL,
  output_url        TEXT NOT NULL,
  shopline_image_id TEXT,
  created_at        TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS imagelingo.usage_logs (
  id                UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  store_id          UUID REFERENCES imagelingo.stores(id),
  month             TEXT NOT NULL,
  images_translated INT DEFAULT 0,
  updated_at        TIMESTAMPTZ DEFAULT NOW(),
  UNIQUE(store_id, month)
);

CREATE TABLE IF NOT EXISTS imagelingo.subscriptions (
  id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  store_id            UUID REFERENCES imagelingo.stores(id) UNIQUE,
  plan                TEXT DEFAULT 'free',
  images_limit        INT DEFAULT 5,
  billing_cycle_start TIMESTAMPTZ,
  updated_at          TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS imagelingo.webhook_events (
  event_id   TEXT PRIMARY KEY,
  received_at TIMESTAMPTZ DEFAULT NOW()
);
