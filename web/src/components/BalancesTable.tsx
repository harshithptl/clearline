import {
  numeric,
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from '@/components/ui/table';
import type { AccountBalances } from '@/lib/api';
import { format, parse } from '@/lib/decimal';
import { cn } from '@/lib/utils';

interface Props {
  accounts: AccountBalances[];
}

/** Every cached account, one column per asset, amounts exactly as the ledger holds them. */
export function BalancesTable({ accounts }: Props) {
  if (accounts.length === 0) {
    return (
      <p className="py-6 text-center text-sm text-muted-foreground">
        No balances yet. Place a crossing buy and sell to create a trade.
      </p>
    );
  }
  const assets = [...new Set(accounts.flatMap((a) => Object.keys(a.balances)))].sort(byQuoteFirst);
  return (
    <Table aria-label="Balances">
      <TableHeader>
        <TableRow className="hover:bg-transparent">
          <TableHead scope="col">Account</TableHead>
          {assets.map((asset) => (
            <TableHead key={asset} scope="col" className="text-right">
              {asset}
            </TableHead>
          ))}
          <TableHead scope="col" className="text-right">
            Events
          </TableHead>
        </TableRow>
      </TableHeader>
      <TableBody>
        {accounts.map((a) => (
          <TableRow key={a.account_id}>
            <TableCell className="font-medium">{a.account_id}</TableCell>
            {assets.map((asset) => {
              const raw = a.balances[asset];
              return (
                <TableCell key={asset} className={cn(numeric, tone(raw))}>
                  {raw === undefined ? '' : format(parse(raw))}
                </TableCell>
              );
            })}
            <TableCell className={cn(numeric, 'text-muted-foreground')}>{a.events}</TableCell>
          </TableRow>
        ))}
      </TableBody>
    </Table>
  );
}

// Quote currency first, then symbols alphabetically.
function byQuoteFirst(a: string, b: string): number {
  const qa = a === 'USD' ? 0 : 1;
  const qb = b === 'USD' ? 0 : 1;
  return qa - qb || a.localeCompare(b);
}

function tone(raw: string | undefined): string {
  if (raw === undefined) return '';
  const v = parse(raw);
  return v < 0n ? 'text-negative' : v > 0n ? 'text-positive' : '';
}
