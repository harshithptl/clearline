-- 0001_init: core ledger schema.
-- Money is NUMERIC, never float. The ledger is append-only: corrections are
-- new reversing entries, so UPDATE, DELETE and TRUNCATE are rejected below.

CREATE TABLE accounts (
    account_id  TEXT PRIMARY KEY CHECK (account_id <> ''),
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Double-entry ledger. Signed amount: negative = debit, positive = credit.
CREATE TABLE ledger_entries (
    entry_id    BIGSERIAL PRIMARY KEY,
    trade_id    TEXT NOT NULL CHECK (trade_id <> ''),
    account_id  TEXT NOT NULL REFERENCES accounts(account_id),
    asset       TEXT NOT NULL CHECK (asset <> ''),
    amount      NUMERIC(20,8) NOT NULL CHECK (amount <> 0),
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX ledger_entries_account_asset_idx ON ledger_entries (account_id, asset);
CREATE INDEX ledger_entries_trade_idx ON ledger_entries (trade_id);

-- Enforce append-only in the database, not just by convention.
CREATE FUNCTION ledger_entries_append_only() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'ledger_entries is append-only: % is not allowed, post a reversing entry instead', TG_OP
        USING ERRCODE = 'restrict_violation';
END;
$$;

CREATE TRIGGER ledger_entries_no_update_delete
    BEFORE UPDATE OR DELETE ON ledger_entries
    FOR EACH ROW EXECUTE FUNCTION ledger_entries_append_only();

CREATE TRIGGER ledger_entries_no_truncate
    BEFORE TRUNCATE ON ledger_entries
    FOR EACH STATEMENT EXECUTE FUNCTION ledger_entries_append_only();

-- Dedupe table: one row per processed event. The primary key is the dedupe.
CREATE TABLE processed_events (
    event_id     TEXT PRIMARY KEY CHECK (event_id <> ''),
    processed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Transactional outbox. Written in the same transaction as ledger rows;
-- the relay publishes rows and sets published_at.
CREATE TABLE outbox (
    outbox_id    BIGSERIAL PRIMARY KEY,
    event_id     TEXT NOT NULL UNIQUE CHECK (event_id <> ''),
    topic        TEXT NOT NULL CHECK (topic <> ''),
    key          TEXT NOT NULL CHECK (key <> ''),
    payload      JSONB NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at TIMESTAMPTZ
);

-- The relay scans for unpublished rows in order; keep that scan cheap.
CREATE INDEX outbox_unpublished_idx ON outbox (outbox_id) WHERE published_at IS NULL;
