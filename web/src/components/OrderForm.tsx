import { CheckCircle2, RotateCcw, XCircle } from 'lucide-react';
import { useState, type SyntheticEvent } from 'react';

import { Button } from '@/components/ui/button';
import { Field } from '@/components/ui/field';
import { Input } from '@/components/ui/input';
import { Segmented, SegmentedItem } from '@/components/ui/segmented';
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select';
import { placeOrder, type OrderRequest, type OrderResult, type SymbolInfo } from '@/lib/api';
import { isPositiveDecimal } from '@/lib/decimal';
import { cn } from '@/lib/utils';

interface Props {
  symbols: SymbolInfo[];
  /** Injectable for tests. */
  newKey?: () => string;
}

const ACCOUNT = /^[a-z0-9_-]{1,64}$/;
type Errors = Partial<Record<'account_id' | 'quantity' | 'price', string>>;

/**
 * Places a limit order through the gateway. The Idempotency-Key belongs to
 * the form's current contents: submitting the same order again reuses it
 * (the gateway replays the first response instead of ordering twice), and
 * editing any field starts a new key.
 */
export function OrderForm({ symbols, newKey = () => crypto.randomUUID() }: Props) {
  const [order, setOrder] = useState<OrderRequest>({
    account_id: '',
    symbol: symbols[0]?.symbol ?? '',
    side: 'buy',
    quantity: '1',
    price: '',
  });
  const [key, setKey] = useState(newKey);
  const [touched, setTouched] = useState(false);
  const [pending, setPending] = useState(false);
  const [result, setResult] = useState<OrderResult | null>(null);

  const symbol = symbols.find((s) => s.symbol === order.symbol);
  const errors = validate(order);
  const valid = Object.keys(errors).length === 0;
  const shown = touched ? errors : {};

  function update<K extends keyof OrderRequest>(field: K, value: OrderRequest[K]) {
    setOrder((o) => ({ ...o, [field]: value }));
    setKey(newKey());
    setResult(null);
  }

  async function submit(e: SyntheticEvent<HTMLFormElement>) {
    e.preventDefault();
    setTouched(true);
    if (!valid) return;
    setPending(true);
    try {
      setResult(await placeOrder(order, key));
    } catch (err) {
      setResult({
        status: 0,
        replayed: false,
        error: err instanceof Error ? err.message : 'failed',
      });
    } finally {
      setPending(false);
    }
  }

  return (
    <form
      className="grid gap-4"
      onSubmit={(e) => void submit(e)}
      aria-label="Place order"
      noValidate
    >
      <Field id="account" label="Account" error={shown.account_id}>
        <Input
          id="account"
          value={order.account_id}
          onChange={(e) => {
            update('account_id', e.target.value);
          }}
          placeholder="alice"
          autoComplete="off"
          aria-invalid={shown.account_id !== undefined}
          aria-describedby={shown.account_id ? 'account-desc' : undefined}
        />
      </Field>

      <div className="grid grid-cols-2 gap-3">
        <Field id="symbol" label="Symbol">
          <Select
            value={order.symbol}
            onValueChange={(v) => {
              update('symbol', v);
            }}
          >
            <SelectTrigger id="symbol" aria-label="Symbol">
              <SelectValue />
            </SelectTrigger>
            <SelectContent>
              {symbols.map((s) => (
                <SelectItem key={s.symbol} value={s.symbol}>
                  {s.symbol}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
        </Field>
        <Field id="side" label="Side">
          <Segmented
            id="side"
            aria-label="Side"
            value={order.side}
            onValueChange={(v) => {
              update('side', v === 'sell' ? 'sell' : 'buy');
            }}
          >
            <SegmentedItem value="buy">buy</SegmentedItem>
            <SegmentedItem value="sell">sell</SegmentedItem>
          </Segmented>
        </Field>
      </div>

      <div className="grid grid-cols-2 gap-3">
        <Field id="quantity" label="Quantity" error={shown.quantity}>
          <Input
            id="quantity"
            value={order.quantity}
            onChange={(e) => {
              update('quantity', e.target.value);
            }}
            inputMode="numeric"
            className="font-mono"
            aria-invalid={shown.quantity !== undefined}
            aria-describedby={shown.quantity ? 'quantity-desc' : undefined}
          />
        </Field>
        <Field
          id="price"
          label={symbol ? `Price (${symbol.quote_asset})` : 'Price'}
          hint={symbol ? `tick ${symbol.tick_size}` : ''}
          error={shown.price}
        >
          <Input
            id="price"
            value={order.price}
            onChange={(e) => {
              update('price', e.target.value);
            }}
            inputMode="decimal"
            className="font-mono"
            placeholder={symbol ? `${symbol.min_price} to ${symbol.max_price}` : ''}
            aria-invalid={shown.price !== undefined}
            aria-describedby="price-desc"
          />
        </Field>
      </div>

      <Button type="submit" disabled={pending} className="capitalize">
        {pending ? 'Sending...' : `Place ${order.side}`}
      </Button>

      <p className="flex items-center gap-1.5 text-xs text-muted-foreground">
        <RotateCcw className="size-3.5 shrink-0" aria-hidden="true" />
        <span>
          Idempotency-Key <code className="text-foreground">{key.slice(0, 8)}</code>. Submit again
          without editing and the gateway replays the first response.
        </span>
      </p>

      {result && <Result result={result} />}
    </form>
  );
}

function validate(o: OrderRequest): Errors {
  const e: Errors = {};
  if (!ACCOUNT.test(o.account_id)) e.account_id = '1 to 64 of a-z, 0-9, _ or -';
  if (!/^\d+$/.test(o.quantity) || !isPositiveDecimal(o.quantity))
    e.quantity = 'A whole number above 0';
  if (!isPositiveDecimal(o.price)) e.price = 'A positive decimal';
  return e;
}

function Result({ result }: { result: OrderResult }) {
  const ok = result.status === 202;
  return (
    <div
      role={ok ? 'status' : 'alert'}
      className={cn(
        'flex items-start gap-2 rounded-md px-3 py-2 text-sm break-all',
        ok ? 'bg-positive-muted text-positive' : 'bg-negative-muted text-negative',
      )}
    >
      {ok ? (
        <CheckCircle2 className="mt-0.5 size-4 shrink-0" aria-hidden="true" />
      ) : (
        <XCircle className="mt-0.5 size-4 shrink-0" aria-hidden="true" />
      )}
      {ok ? (
        <span>
          Accepted <code>{result.orderId}</code>
          {result.replayed && ' (replayed: same key, no second order)'}
        </span>
      ) : (
        <span>
          {result.status === 0 ? 'Network error' : `Rejected (${result.status})`}: {result.error}
        </span>
      )}
    </div>
  );
}
