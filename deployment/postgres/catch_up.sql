ALTER TABLE public.sessions ADD COLUMN IF NOT EXISTS survey_country text;

CREATE TABLE IF NOT EXISTS host_country_stats (
    host       TEXT NOT NULL,
    country    TEXT NOT NULL,
    pct        NUMERIC(5,1) NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (host, country)
);

ALTER TABLE incidents ADD COLUMN IF NOT EXISTS first_responder_processed BOOLEAN DEFAULT FALSE;

CREATE TABLE IF NOT EXISTS first_responder_actions (
    id           BIGSERIAL PRIMARY KEY,
    incident_id  BIGINT NOT NULL REFERENCES incidents(id),
    host         TEXT NOT NULL,
    action       TEXT NOT NULL,
    target       TEXT NOT NULL DEFAULT '',
    confidence   TEXT NOT NULL,
    reasoning    TEXT,
    ttl_minutes  INTEGER NOT NULL DEFAULT 30,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    expires_at   TIMESTAMPTZ NOT NULL,
    applied      BOOLEAN NOT NULL DEFAULT FALSE
);
CREATE INDEX IF NOT EXISTS first_responder_actions_host_expires
    ON first_responder_actions (host, expires_at DESC);
