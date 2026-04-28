-- Schema for the Postgres store backend. Applied idempotently by
-- Store.EnsureIndexes() via pgx.Exec; safe to run repeatedly.

CREATE TABLE IF NOT EXISTS transactions (
    txid            TEXT PRIMARY KEY,
    status          TEXT NOT NULL,
    status_code     INT,
    block_hash      TEXT,
    block_height    BIGINT,
    merkle_path     BYTEA,
    extra_info      TEXT,
    competing_txs   JSONB,
    raw_tx          BYTEA,
    retry_count     INT NOT NULL DEFAULT 0,
    next_retry_at   TIMESTAMPTZ,
    timestamp_at    TIMESTAMPTZ NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_tx_status        ON transactions(status);
CREATE INDEX IF NOT EXISTS idx_tx_block_hash    ON transactions(block_hash);
CREATE INDEX IF NOT EXISTS idx_tx_updated       ON transactions(timestamp_at);
-- Partial index keeps the reaper's hot query from scanning the whole
-- transactions table — only the handful of rows currently in the retry
-- state are indexed.
CREATE INDEX IF NOT EXISTS idx_tx_retry_ready
    ON transactions(next_retry_at)
    WHERE status = 'PENDING_RETRY';

CREATE TABLE IF NOT EXISTS bumps (
    block_hash   TEXT PRIMARY KEY,
    block_height BIGINT NOT NULL,
    bump_data    BYTEA NOT NULL
);

CREATE TABLE IF NOT EXISTS stumps (
    block_hash    TEXT NOT NULL,
    subtree_index INT NOT NULL,
    stump_data    BYTEA NOT NULL,
    PRIMARY KEY (block_hash, subtree_index)
);
CREATE INDEX IF NOT EXISTS idx_stump_block_hash ON stumps(block_hash);

CREATE TABLE IF NOT EXISTS submissions (
    submission_id         TEXT PRIMARY KEY,
    txid                  TEXT NOT NULL,
    callback_url          TEXT,
    callback_token        TEXT,
    full_status_updates   BOOLEAN NOT NULL DEFAULT FALSE,
    last_delivered_status TEXT,
    retry_count           INT NOT NULL DEFAULT 0,
    next_retry_at         TIMESTAMPTZ,
    created_at            TIMESTAMPTZ NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_sub_txid   ON submissions(txid);
CREATE INDEX IF NOT EXISTS idx_sub_token  ON submissions(callback_token);

CREATE TABLE IF NOT EXISTS leases (
    name       TEXT PRIMARY KEY,
    holder     TEXT NOT NULL,
    expires_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS datahub_endpoints (
    url        TEXT PRIMARY KEY,
    source     TEXT NOT NULL,
    last_seen  TIMESTAMPTZ NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_dh_last_seen ON datahub_endpoints(last_seen);
