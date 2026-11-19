-- +goose Up
-- 0004_account_id_format: account ids are 1 to 64 of [a-z0-9_-].
--
-- Same rule as ledger.AccountIDPattern, checked by the gateway at the edge and
-- by settlement. Enforcing it here too means no code path can create an
-- account the rest of the system would reject. It subsumes 0003 (no ':'),
-- which stays in place: applied migrations are never edited.
--
-- Fails if an existing account breaks the rule; fix or recreate those first.

ALTER TABLE accounts
    ADD CONSTRAINT accounts_id_format CHECK (account_id ~ '^[a-z0-9_-]{1,64}$');
