-- +goose Up
-- 0002_outbox_per_account: one ledger event per account touched by a trade.
--
-- ledger-events is keyed by account so each account's updates stay in order
-- on one partition. A trade touches two accounts, and a Kafka message has one
-- key, so settlement writes one outbox row per account (e.g. event_id
-- "T42:alice" and "T42:bob"), all in the same transaction as the ledger rows.
--
-- source_event_id links each outbox row back to the processed event (trade)
-- that produced it, so invariant 5 can be checked without parsing event_id.
-- Pre-production: outbox is always empty when this runs, so NOT NULL without
-- a default is safe.

ALTER TABLE outbox
    ADD COLUMN source_event_id TEXT NOT NULL CHECK (source_event_id <> '');

-- At most one event per (trade, topic, key): a trade can never emit two
-- balance updates for the same account.
ALTER TABLE outbox
    ADD CONSTRAINT outbox_source_topic_key_unique UNIQUE (source_event_id, topic, key);
