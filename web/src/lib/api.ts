// Typed client for the Clearline HTTP APIs. All requests go to the page's
// own origin under /api/*; nginx (and the Vite dev server) forward them to
// the services, so the browser never needs CORS.
//
// Responses are checked at runtime before use: the type annotations alone
// would only be a promise the server might not keep.

export const API = {
  gateway: '/api/gateway',
  balance: '/api/balance',
  reconciler: '/api/reconciler',
} as const;

export interface Change {
  asset: string;
  amount: string;
}

export interface LedgerEvent {
  version: number;
  event_id: string;
  trade_id: string;
  account_id: string;
  changes: Change[];
}

export interface AccountBalances {
  account_id: string;
  balances: Record<string, string>;
  events: number;
}

export type ReconcilerState = 'pending' | 'clean' | 'lagging' | 'mismatch' | 'error';

export interface ReconcilerStatus {
  state: ReconcilerState;
  checked_at: string | null;
  accounts: number;
  cache_mismatches: number;
  lagging: number;
  error?: string;
}

export interface SymbolInfo {
  symbol: string;
  quote_asset: string;
  tick_size: string;
  min_price: string;
  max_price: string;
}

export interface OrderRequest {
  account_id: string;
  symbol: string;
  side: 'buy' | 'sell';
  quantity: string;
  price: string;
}

export interface OrderResult {
  status: number;
  replayed: boolean;
  orderId?: string;
  error?: string;
}

function isRecord(v: unknown): v is Record<string, unknown> {
  return typeof v === 'object' && v !== null && !Array.isArray(v);
}

function isStringMap(v: unknown): v is Record<string, string> {
  return isRecord(v) && Object.values(v).every((x) => typeof x === 'string');
}

export function isLedgerEvent(v: unknown): v is LedgerEvent {
  return (
    isRecord(v) &&
    typeof v.event_id === 'string' &&
    typeof v.trade_id === 'string' &&
    typeof v.account_id === 'string' &&
    Array.isArray(v.changes) &&
    v.changes.every(
      (c: unknown) => isRecord(c) && typeof c.asset === 'string' && typeof c.amount === 'string',
    )
  );
}

function isAccountBalances(v: unknown): v is AccountBalances {
  return (
    isRecord(v) &&
    typeof v.account_id === 'string' &&
    isStringMap(v.balances) &&
    typeof v.events === 'number'
  );
}

const states: readonly string[] = ['pending', 'clean', 'lagging', 'mismatch', 'error'];

function isReconcilerStatus(v: unknown): v is ReconcilerStatus {
  return isRecord(v) && typeof v.state === 'string' && states.includes(v.state);
}

function isSymbolInfo(v: unknown): v is SymbolInfo {
  return (
    isRecord(v) &&
    typeof v.symbol === 'string' &&
    typeof v.quote_asset === 'string' &&
    typeof v.tick_size === 'string' &&
    typeof v.min_price === 'string' &&
    typeof v.max_price === 'string'
  );
}

async function getJSON(url: string): Promise<unknown> {
  const res = await fetch(url, { headers: { Accept: 'application/json' } });
  if (!res.ok) throw new Error(`${url}: HTTP ${res.status}`);
  return (await res.json()) as unknown;
}

export async function fetchBalances(): Promise<AccountBalances[]> {
  const body = await getJSON(`${API.balance}/balances`);
  if (!isRecord(body) || !Array.isArray(body.accounts) || !body.accounts.every(isAccountBalances)) {
    throw new Error('unexpected /balances response');
  }
  return body.accounts;
}

export async function fetchStatus(): Promise<ReconcilerStatus> {
  const body = await getJSON(`${API.reconciler}/status`);
  if (!isReconcilerStatus(body)) throw new Error('unexpected /status response');
  return body;
}

/** The order form is only offered where the gateway is routed (never on the public demo). */
export async function fetchSymbols(): Promise<SymbolInfo[] | null> {
  let res: Response;
  try {
    res = await fetch(`${API.gateway}/symbols`, { headers: { Accept: 'application/json' } });
  } catch {
    return null;
  }
  if (!res.ok) return null;
  const body = (await res.json()) as unknown;
  if (!Array.isArray(body) || !body.every(isSymbolInfo)) return null;
  return body;
}

export async function placeOrder(
  order: OrderRequest,
  idempotencyKey: string,
): Promise<OrderResult> {
  const res = await fetch(`${API.gateway}/orders`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', 'Idempotency-Key': idempotencyKey },
    body: JSON.stringify(order),
  });
  const replayed = res.headers.get('Idempotent-Replayed') === 'true';
  let body: unknown = null;
  try {
    body = (await res.json()) as unknown;
  } catch {
    // non-JSON error page from a proxy; reported by status below
  }
  const result: OrderResult = { status: res.status, replayed };
  if (isRecord(body) && typeof body.order_id === 'string') result.orderId = body.order_id;
  if (isRecord(body) && typeof body.error === 'string') result.error = body.error;
  else if (!res.ok) result.error = `HTTP ${res.status}`;
  return result;
}
