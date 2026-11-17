-- +goose Up
-- 0003_account_id_no_colon: make per-account ledger event IDs unambiguous.
--
-- Ledger event IDs are "<trade_id>:<account_id>". If account IDs could
-- contain ':', two different pairs could collide:
--   trade "a"   + account "b:c"  ->  "a:b:c"
--   trade "a:b" + account "c"    ->  "a:b:c"
-- Forbidding ':' in account IDs means everything after the last ':' is the
-- account, so every (trade, account) pair maps to a distinct event ID.

ALTER TABLE accounts
    ADD CONSTRAINT accounts_id_no_colon CHECK (strpos(account_id, ':') = 0);
