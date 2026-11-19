import { render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { describe, expect, it, vi } from 'vitest';

import { OrderForm } from '@/components/OrderForm';
import type { SymbolInfo } from '@/lib/api';

const symbols: SymbolInfo[] = [
  { symbol: 'AAPL', quote_asset: 'USD', tick_size: '0.01', min_price: '0.01', max_price: '1150' },
  { symbol: 'MSFT', quote_asset: 'USD', tick_size: '0.01', min_price: '0.01', max_price: '1400' },
];

function keys() {
  let n = 0;
  return () => `key-${++n}`;
}

function mockGateway(status: number, body: unknown, replayed = false) {
  const fetchMock = vi.fn(() =>
    Promise.resolve(
      new Response(JSON.stringify(body), {
        status,
        headers: replayed ? { 'Idempotent-Replayed': 'true' } : {},
      }),
    ),
  );
  vi.stubGlobal('fetch', fetchMock);
  return fetchMock;
}

function sentKey(fetchMock: ReturnType<typeof mockGateway>, call: number): string | null {
  const init = fetchMock.mock.calls[call] as unknown as [string, RequestInit] | undefined;
  return new Headers(init?.[1].headers).get('Idempotency-Key');
}

async function fill(user: ReturnType<typeof userEvent.setup>) {
  await user.type(screen.getByLabelText('Account'), 'alice');
  await user.clear(screen.getByLabelText('Quantity'));
  await user.type(screen.getByLabelText('Quantity'), '10');
  await user.type(screen.getByLabelText(/^Price/), '150.25');
}

describe('OrderForm', () => {
  it('posts the order with an Idempotency-Key and shows the accepted order id', async () => {
    const fetchMock = mockGateway(202, { order_id: 'o-abc', status: 'accepted' });
    const user = userEvent.setup();
    render(<OrderForm symbols={symbols} newKey={keys()} />);
    await fill(user);
    await user.click(screen.getByRole('button', { name: /place buy/i }));

    expect(await screen.findByRole('status')).toHaveTextContent('Accepted o-abc');
    const [url, init] = fetchMock.mock.calls[0] as unknown as [string, RequestInit];
    expect(url).toBe('/api/gateway/orders');
    expect(JSON.parse(init.body as string)).toEqual({
      account_id: 'alice',
      symbol: 'AAPL',
      side: 'buy',
      quantity: '10',
      price: '150.25',
    });
    expect(sentKey(fetchMock, 0)).toMatch(/^key-/);
  });

  it('reuses the key when the same order is submitted again, and shows the replay', async () => {
    const fetchMock = mockGateway(202, { order_id: 'o-abc' });
    const user = userEvent.setup();
    render(<OrderForm symbols={symbols} newKey={keys()} />);
    await fill(user);
    const submit = screen.getByRole('button', { name: /place buy/i });
    await user.click(submit);
    await screen.findByRole('status');

    mockGateway(202, { order_id: 'o-abc' }, true);
    await user.click(submit);
    expect(await screen.findByText(/replayed: same key/)).toBeInTheDocument();
    const second = vi.mocked(fetch).mock.calls[0] as unknown as [string, RequestInit];
    expect(new Headers(second[1].headers).get('Idempotency-Key')).toBe(sentKey(fetchMock, 0));
  });

  it('starts a new key when the order is edited', async () => {
    const fetchMock = mockGateway(202, { order_id: 'o-1' });
    const user = userEvent.setup();
    render(<OrderForm symbols={symbols} newKey={keys()} />);
    await fill(user);
    await user.click(screen.getByRole('button', { name: /place buy/i }));
    await screen.findByRole('status');

    await user.click(screen.getByRole('radio', { name: 'sell' }));
    await user.click(screen.getByRole('button', { name: /place sell/i }));
    await screen.findByRole('status');
    expect(sentKey(fetchMock, 1)).not.toBe(sentKey(fetchMock, 0));
    const body = JSON.parse(
      (fetchMock.mock.calls[1] as unknown as [string, RequestInit])[1].body as string,
    ) as { side: string };
    expect(body.side).toBe('sell');
  });

  it('shows field errors instead of sending an invalid order', async () => {
    const fetchMock = mockGateway(202, {});
    const user = userEvent.setup();
    render(<OrderForm symbols={symbols} newKey={keys()} />);
    await user.type(screen.getByLabelText('Account'), 'Alice!');
    await user.click(screen.getByRole('button', { name: /place buy/i }));

    expect(screen.getByLabelText('Account')).toHaveAttribute('aria-invalid', 'true');
    expect(screen.getByText('1 to 64 of a-z, 0-9, _ or -')).toBeInTheDocument();
    expect(screen.getByText('A positive decimal')).toBeInTheDocument();
    expect(fetchMock).not.toHaveBeenCalled();
  });

  it('shows the gateway rejection', async () => {
    mockGateway(422, { error: 'unknown account "alice"' });
    const user = userEvent.setup();
    render(<OrderForm symbols={symbols} newKey={keys()} />);
    await fill(user);
    await user.click(screen.getByRole('button', { name: /place buy/i }));
    expect(await screen.findByRole('alert')).toHaveTextContent('Rejected (422): unknown account');
  });
});
