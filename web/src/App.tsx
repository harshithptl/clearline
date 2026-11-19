import { useQuery, useQueryClient } from '@tanstack/react-query';
import { Radio, WifiOff } from 'lucide-react';
import { useCallback, useEffect, useMemo, useState } from 'react';

import { BalancesTable } from '@/components/BalancesTable';
import { OrderForm } from '@/components/OrderForm';
import { StatusBadge } from '@/components/StatusBadge';
import { TradeTape } from '@/components/TradeTape';
import { Badge } from '@/components/ui/badge';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card';
import { fetchBalances, fetchStatus, fetchSymbols, type LedgerEvent } from '@/lib/api';
import { throttle } from '@/lib/throttle';
import { addToTape, type Trade } from '@/lib/trades';
import { useLedgerStream, type StreamState } from '@/lib/useLedgerStream';

export function App() {
  const queryClient = useQueryClient();
  const [tape, setTape] = useState<Trade[]>([]);

  const balances = useQuery({
    queryKey: ['balances'],
    queryFn: fetchBalances,
    refetchInterval: 15_000, // backstop; the stream normally triggers refetches
  });
  const status = useQuery({ queryKey: ['status'], queryFn: fetchStatus, refetchInterval: 5_000 });
  // null: the gateway is not routed here (public demo), so no order form.
  const symbols = useQuery({ queryKey: ['symbols'], queryFn: fetchSymbols, staleTime: Infinity });

  // At most one balances refetch per second, and never cancel one in flight:
  // the stream can carry hundreds of events a second.
  const balancesRefresh = useMemo(
    () =>
      throttle(() => {
        void queryClient.invalidateQueries({ queryKey: ['balances'] }, { cancelRefetch: false });
      }, 1000),
    [queryClient],
  );
  useEffect(() => balancesRefresh.cancel, [balancesRefresh]);
  const refreshBalances = balancesRefresh.call;
  const onEvent = useCallback(
    (ev: LedgerEvent) => {
      setTape((t) => addToTape(t, ev));
      refreshBalances();
    },
    [refreshBalances],
  );
  const stream = useLedgerStream(onEvent, refreshBalances);
  const tradable = symbols.data ?? [];

  return (
    <div className="mx-auto max-w-7xl px-4 py-6 sm:px-6 lg:py-10">
      <header className="mb-6 flex flex-wrap items-center justify-between gap-3">
        <div>
          <h1 className="text-2xl font-semibold tracking-tight">Clearline</h1>
          <p className="text-sm text-muted-foreground">
            Trade settlement and double-entry ledger, live
          </p>
        </div>
        <div className="flex items-center gap-2">
          <StatusBadge status={status.data} unreachable={status.isError} />
          <StreamBadge state={stream} />
        </div>
      </header>

      <main className="grid gap-4 lg:grid-cols-3">
        <Card aria-labelledby="balances-title" className="lg:col-span-2 lg:row-span-2">
          <CardHeader>
            <div>
              <CardTitle id="balances-title">Balances</CardTitle>
              <CardDescription>
                From the Valkey cache, checked against the ledger by the reconciler.
              </CardDescription>
            </div>
          </CardHeader>
          <CardContent>
            {balances.isError ? (
              <p className="py-6 text-center text-sm text-negative">Balance service unreachable.</p>
            ) : (
              <BalancesTable accounts={balances.data ?? []} />
            )}
          </CardContent>
        </Card>

        {tradable.length > 0 && (
          <Card aria-labelledby="order-title">
            <CardHeader>
              <div>
                <CardTitle id="order-title">Place order</CardTitle>
                <CardDescription>Limit order via the gateway.</CardDescription>
              </div>
            </CardHeader>
            <CardContent>
              <OrderForm symbols={tradable} />
            </CardContent>
          </Card>
        )}

        <Card aria-labelledby="tape-title">
          <CardHeader>
            <div>
              <CardTitle id="tape-title">Trade tape</CardTitle>
              <CardDescription>Settled since this page opened, newest first.</CardDescription>
            </div>
          </CardHeader>
          <CardContent>
            <TradeTape trades={tape} />
          </CardContent>
        </Card>
      </main>
    </div>
  );
}

function StreamBadge({ state }: { state: StreamState }) {
  if (state === 'live') {
    return (
      <Badge tone="positive">
        <Radio aria-hidden="true" />
        Live
      </Badge>
    );
  }
  return (
    <Badge tone="warning">
      <WifiOff aria-hidden="true" />
      {state === 'connecting' ? 'Connecting' : 'Reconnecting'}
    </Badge>
  );
}
