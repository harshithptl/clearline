import { ArrowLeft } from 'lucide-react';

import { Badge } from '@/components/ui/badge';
import type { Trade } from '@/lib/trades';

interface Props {
  trades: Trade[];
}

/** Trades as they settle, newest first, built from the live ledger event stream. */
export function TradeTape({ trades }: Props) {
  if (trades.length === 0) {
    return (
      <p className="py-6 text-center text-sm text-muted-foreground">
        Waiting for trades. New settlements appear here live.
      </p>
    );
  }
  return (
    <ol aria-label="Trade tape" className="max-h-[26rem] divide-y overflow-y-auto">
      {trades.map((t) => (
        <li key={t.tradeId} className="grid gap-1 py-2.5 first:pt-0">
          <div className="flex items-center justify-between gap-2">
            <Badge>{t.symbol}</Badge>
            <span className="font-mono text-sm tabular-nums">
              {t.quantity} @ {t.price} {t.quoteAsset}
            </span>
          </div>
          <div className="flex items-center gap-1.5 text-sm text-muted-foreground">
            <span className="text-foreground">{t.buyer ?? '...'}</span>
            <ArrowLeft className="size-3.5" aria-hidden="true" />
            <span className="sr-only">bought from</span>
            <span className="text-foreground">{t.seller ?? '...'}</span>
          </div>
          <code className="truncate text-xs text-muted-foreground">{t.tradeId}</code>
        </li>
      ))}
    </ol>
  );
}
