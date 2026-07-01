-- Schema for the Postgres store backend. Applied idempotently by
-- Store.EnsureIndexes() via pgx.Exec; safe to run repeatedly.

CREATE TABLE IF NOT EXISTS transactions (
    txid                 TEXT PRIMARY KEY,
    status               TEXT NOT NULL,
    status_code          INT,
    block_hash           TEXT,
    block_height         BIGINT,
    merkle_path          BYTEA,
    extra_info           TEXT,
    competing_txs        JSONB,
    raw_tx               BYTEA,
    retry_count          INT NOT NULL DEFAULT 0,
    next_retry_at        TIMESTAMPTZ,
    timestamp_at         TIMESTAMPTZ NOT NULL,
    created_at           TIMESTAMPTZ NOT NULL,
    merkle_registered_at TIMESTAMPTZ,
    last_rebroadcast_at  TIMESTAMPTZ
);

-- Idempotent column add for stores created before merkle_registered_at was
-- introduced. Existing rows keep NULL until the next successful /watch call
-- repopulates the marker — see issue #145.
ALTER TABLE transactions ADD COLUMN IF NOT EXISTS merkle_registered_at TIMESTAMPTZ;

-- Idempotent column add for stores created before last_rebroadcast_at was
-- introduced. NULL means "never rebroadcast", so existing stuck rows become
-- immediately eligible for the reaper's first rebroadcast.
ALTER TABLE transactions ADD COLUMN IF NOT EXISTS last_rebroadcast_at TIMESTAMPTZ;

CREATE INDEX IF NOT EXISTS idx_tx_status        ON transactions(status);
CREATE INDEX IF NOT EXISTS idx_tx_block_hash    ON transactions(block_hash);
CREATE INDEX IF NOT EXISTS idx_tx_updated       ON transactions(timestamp_at);
-- Partial index keeps the reaper's hot query from scanning the whole
-- transactions table — only the handful of rows currently in the retry
-- state are indexed.
CREATE INDEX IF NOT EXISTS idx_tx_retry_ready
    ON transactions(next_retry_at)
    WHERE status = 'PENDING_RETRY';
-- Partial index for the reaper's rebroadcast-candidate scan: only rows in a
-- non-terminal SEEN_* state are eligible, and ordering by last_rebroadcast_at
-- (NULLs first) drives the oldest-unserved-first fairness query.
-- Note: the timestamp_at range and length(raw_tx) > 0 predicates are inline
-- rechecks, not part of this index. At a very large SEEN_* population the scan
-- may touch many index/heap rows before LIMIT is satisfied; if that shows up in
-- query plans, consider a composite/covering index — fine at current scale.
CREATE INDEX IF NOT EXISTS idx_tx_reap_due
    ON transactions(last_rebroadcast_at NULLS FIRST)
    WHERE status IN ('SEEN_ON_NETWORK', 'SEEN_MULTIPLE_NODES');

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

-- Per-block processing status. One row per block hash; tracks the milestones
-- (header observed, BLOCK_PROCESSED received, compound BUMP built) and reorg
-- state. Writers use partial UPDATEs (only their own column on conflict) so
-- concurrent paths converge correctly.
CREATE TABLE IF NOT EXISTS block_processing (
    block_hash     TEXT PRIMARY KEY,
    block_height   BIGINT NOT NULL,
    header_seen_at TIMESTAMPTZ NOT NULL,
    processed_at   TIMESTAMPTZ,
    bump_built_at  TIMESTAMPTZ,
    status         TEXT NOT NULL DEFAULT 'active',
    orphaned_at    TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS idx_bp_block_height  ON block_processing(block_height DESC);
CREATE INDEX IF NOT EXISTS idx_bp_status_height ON block_processing(status, block_height DESC);
-- Partial index for the bump-builder watchdog's stale-row scan. Only rows
-- that have observed a header but not yet seen BLOCK_PROCESSED are eligible
-- candidates, so the predicate keeps the index size proportional to the
-- backlog rather than the full block history.
CREATE INDEX IF NOT EXISTS idx_bp_stale_seen
    ON block_processing(header_seen_at)
    WHERE processed_at IS NULL AND status = 'active';

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
-- Idempotent column adds for stores created before the exactly-once webhook
-- delivery columns existed (commit d0a3a39). Without these, a deployed
-- database upgraded in place would silently miss last_delivered_status and
-- the CAS predicate would match no rows, leaving WebhookCASLostTotal as
-- the only outward symptom.
ALTER TABLE submissions ADD COLUMN IF NOT EXISTS last_delivered_status TEXT;
ALTER TABLE submissions ADD COLUMN IF NOT EXISTS retry_count           INT NOT NULL DEFAULT 0;
ALTER TABLE submissions ADD COLUMN IF NOT EXISTS next_retry_at         TIMESTAMPTZ;
CREATE INDEX IF NOT EXISTS idx_sub_txid   ON submissions(txid);
CREATE INDEX IF NOT EXISTS idx_sub_token  ON submissions(callback_token);
-- Partial index keyed off the webhook reaper's scan predicate; stays
-- proportional to the in-retry backlog, not the full submissions table.
CREATE INDEX IF NOT EXISTS idx_sub_retry_ready
    ON submissions(next_retry_at)
    WHERE retry_count > 0;

CREATE TABLE IF NOT EXISTS leases (
    name       TEXT PRIMARY KEY,
    holder     TEXT NOT NULL,
    expires_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS datahub_endpoints (
    url        TEXT PRIMARY KEY,
    network    TEXT NOT NULL DEFAULT '',
    source     TEXT NOT NULL,
    last_seen  TIMESTAMPTZ NOT NULL
);
-- Idempotent column add for tables created before the network scoping was
-- introduced. Existing rows keep the empty default, which excludes them from
-- every ListDatahubEndpoints query — they re-register the next time the peer
-- announces, with the correct network attached.
ALTER TABLE datahub_endpoints ADD COLUMN IF NOT EXISTS network TEXT NOT NULL DEFAULT '';
CREATE INDEX IF NOT EXISTS idx_dh_last_seen ON datahub_endpoints(last_seen);
CREATE INDEX IF NOT EXISTS idx_dh_network   ON datahub_endpoints(network);
