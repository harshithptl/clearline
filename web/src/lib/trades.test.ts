import { describe, expect, it } from 'vitest';

import type { LedgerEvent } from '@/lib/api';
import { addToTape, symbolOf, tradeFromEvent } from '@/lib/trades';

// Alice buys 5 E2E at 100.25 from Bob: one ledger event per account.
const alice: LedgerEvent = {
  version: 1,
  event_id: 'E2E-1-7-1:alice',
  trade_id: 'E2E-1-7-1',
  account_id: 'alice',
  changes: [
    { asset: 'USD', amount: '-501.25' },
    { asset: 'E2E', amount: '5' },
  ],
};
const bob: LedgerEvent = {
  ...alice,
  event_id: 'E2E-1-7-1:bob',
  account_id: 'bob',
  changes: [
    { asset: 'USD', amount: '501.25' },
    { asset: 'E2E', amount: '-5' },
  ],
};

describe('trades', () => {
  it('reads the symbol from the trade id', () => {
    expect(symbolOf('AAPL-0-12-1')).toBe('AAPL');
    expect(symbolOf('nodash')).toBeNull();
  });

  it('derives the trade from either side alone', () => {
    expect(tradeFromEvent(alice)).toEqual({
      tradeId: 'E2E-1-7-1',
      symbol: 'E2E',
      quoteAsset: 'USD',
      quantity: '5',
      price: '100.25',
      buyer: 'alice',
    });
    expect(tradeFromEvent(bob)).toMatchObject({ seller: 'bob', price: '100.25', quantity: '5' });
  });

  it('ignores events that are not trades', () => {
    expect(tradeFromEvent({ ...alice, changes: [{ asset: 'USD', amount: '1' }] })).toBeNull();
    expect(tradeFromEvent({ ...alice, trade_id: 'nodash' })).toBeNull();
  });

  it('merges the two sides into one tape entry, newest first', () => {
    let tape = addToTape([], alice);
    tape = addToTape(tape, bob);
    expect(tape).toHaveLength(1);
    expect(tape[0]).toMatchObject({ buyer: 'alice', seller: 'bob' });

    const next = { ...alice, trade_id: 'E2E-1-8-1', event_id: 'E2E-1-8-1:alice' };
    tape = addToTape(tape, next);
    expect(tape.map((t) => t.tradeId)).toEqual(['E2E-1-8-1', 'E2E-1-7-1']);
  });

  it('caps the tape length', () => {
    let tape = addToTape([], alice, 2);
    for (const n of [2, 3, 4]) {
      tape = addToTape(tape, { ...alice, trade_id: `E2E-1-${n}-1` }, 2);
    }
    expect(tape.map((t) => t.tradeId)).toEqual(['E2E-1-4-1', 'E2E-1-3-1']);
  });
});
