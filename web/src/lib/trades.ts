// Builds a trade tape from the ledger event stream.
//
// Settlement emits one ledger event per account per trade (SPEC 3.4 #6), so a
// trade arrives as two events, usually adjacent but not guaranteed. Either
// event alone is enough to know the trade (quantity, price, one side); the
// second fills in the counterparty.

import type { LedgerEvent } from '@/lib/api';
import { abs, div, format, parse } from '@/lib/decimal';

export interface Trade {
  tradeId: string;
  symbol: string;
  quoteAsset: string;
  quantity: string;
  price: string;
  buyer?: string;
  seller?: string;
}

/** Trade ids are <symbol>-<partition>-<offset>-<fill> (matcher adapter). */
export function symbolOf(tradeId: string): string | null {
  const i = tradeId.indexOf('-');
  return i > 0 ? tradeId.slice(0, i) : null;
}

/** Reads one side of a trade from one account's ledger event, or null if it is not a trade. */
export function tradeFromEvent(ev: LedgerEvent): Trade | null {
  const symbol = symbolOf(ev.trade_id);
  if (!symbol || ev.changes.length !== 2) return null;
  const base = ev.changes.find((c) => c.asset === symbol);
  const quote = ev.changes.find((c) => c.asset !== symbol);
  if (!base || !quote) return null;
  const qty = parse(base.amount);
  const notional = parse(quote.amount);
  if (qty === 0n) return null;
  const trade: Trade = {
    tradeId: ev.trade_id,
    symbol,
    quoteAsset: quote.asset,
    quantity: format(abs(qty)),
    price: format(div(abs(notional), abs(qty))),
  };
  // The buyer receives the symbol; the seller gives it up.
  if (qty > 0n) trade.buyer = ev.account_id;
  else trade.seller = ev.account_id;
  return trade;
}

/** Adds ev to the tape (newest first, at most max trades), merging the second side of a trade. */
export function addToTape(tape: readonly Trade[], ev: LedgerEvent, max = 50): Trade[] {
  const side = tradeFromEvent(ev);
  if (!side) return [...tape];
  const i = tape.findIndex((t) => t.tradeId === side.tradeId);
  if (i < 0) return [side, ...tape].slice(0, max);
  const existing = tape[i];
  if (!existing) return [...tape];
  const merged: Trade = { ...existing, ...pickSides(existing, side) };
  return tape.map((t, j) => (j === i ? merged : t));
}

function pickSides(a: Trade, b: Trade): Pick<Trade, 'buyer' | 'seller'> {
  const out: Pick<Trade, 'buyer' | 'seller'> = {};
  const buyer = a.buyer ?? b.buyer;
  const seller = a.seller ?? b.seller;
  if (buyer !== undefined) out.buyer = buyer;
  if (seller !== undefined) out.seller = seller;
  return out;
}
