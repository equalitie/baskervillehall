ALTER TABLE public.sessions ADD COLUMN IF NOT EXISTS survey_country text;

CREATE TABLE IF NOT EXISTS host_country_stats (
    host       TEXT NOT NULL,
    country    TEXT NOT NULL,
    pct        NUMERIC(5,1) NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (host, country)
);

ALTER TABLE sessions ADD COLUMN IF NOT EXISTS top_url TEXT;

CREATE TABLE IF NOT EXISTS incident_url_stats (
    incident_id  BIGINT  NOT NULL REFERENCES incidents(id) ON DELETE CASCADE,
    url          TEXT    NOT NULL,
    cmd_count    INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (incident_id, url)
);
CREATE INDEX IF NOT EXISTS incident_url_stats_incident_id
    ON incident_url_stats (incident_id);

CREATE TABLE IF NOT EXISTS incident_ua_stats (
    incident_id  BIGINT  NOT NULL REFERENCES incidents(id) ON DELETE CASCADE,
    ua           TEXT    NOT NULL,
    cmd_count    INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (incident_id, ua)
);
CREATE INDEX IF NOT EXISTS incident_ua_stats_incident_id
    ON incident_ua_stats (incident_id);

CREATE TABLE IF NOT EXISTS host_url_stats (
    host       TEXT NOT NULL,
    url        TEXT NOT NULL,
    pct        NUMERIC(5,1) NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (host, url)
);

CREATE TABLE IF NOT EXISTS host_ua_stats (
    host       TEXT NOT NULL,
    ua         TEXT NOT NULL,
    pct        NUMERIC(5,1) NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (host, ua)
);

CREATE TABLE IF NOT EXISTS host_asn_stats (
    host       TEXT NOT NULL,
    asn_name   TEXT NOT NULL,
    pct        NUMERIC(5,1) NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (host, asn_name)
);

ALTER TABLE incidents ADD COLUMN IF NOT EXISTS first_responder_processed BOOLEAN DEFAULT FALSE;
ALTER TABLE incidents ADD COLUMN IF NOT EXISTS avg_api_ratio      FLOAT DEFAULT 0.0;
ALTER TABLE incidents ADD COLUMN IF NOT EXISTS avg_path_diversity FLOAT DEFAULT 1.0;
ALTER TABLE incidents ADD COLUMN IF NOT EXISTS behavior_samples   INTEGER DEFAULT 0;
ALTER TABLE incidents ADD COLUMN IF NOT EXISTS traffic_peak_ratio   FLOAT DEFAULT 0.0;
ALTER TABLE incidents ADD COLUMN IF NOT EXISTS traffic_close_ratio  FLOAT DEFAULT 0.0;
ALTER TABLE incidents ADD COLUMN IF NOT EXISTS traffic_recent_ratio FLOAT DEFAULT 0.0;
ALTER TABLE incidents ADD COLUMN IF NOT EXISTS traffic_peak_count   INTEGER DEFAULT 0;
ALTER TABLE incidents ADD COLUMN IF NOT EXISTS traffic_recent_count INTEGER DEFAULT 0;
ALTER TABLE incidents ADD COLUMN IF NOT EXISTS traffic_close_count  INTEGER DEFAULT 0;

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

CREATE TABLE IF NOT EXISTS incident_fingerprint_stats (
    incident_id   BIGINT  NOT NULL REFERENCES incidents(id) ON DELETE CASCADE,
    fingerprint   TEXT    NOT NULL,
    cmd_count     INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (incident_id, fingerprint)
);
CREATE INDEX IF NOT EXISTS incident_fingerprint_stats_incident_id
    ON incident_fingerprint_stats (incident_id);

ALTER TABLE incidents ADD COLUMN IF NOT EXISTS narrative TEXT;
