## What

## Why

## Tests
- [ ] `make test` green (stack up via `make init`)
- [ ] New behavior has a test that fails without the change

## Checklist
- [ ] Durability unchanged (Kafka `acks=all`, Postgres `synchronous_commit=on`)
- [ ] Money stays exact (`NUMERIC` / decimal, never float)
- [ ] Any performance number states hardware and settings
- [ ] Docs updated (README, SPEC, decision note) if behavior changed
