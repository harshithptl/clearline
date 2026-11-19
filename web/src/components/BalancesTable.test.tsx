import { render, screen, within } from '@testing-library/react';
import { describe, expect, it } from 'vitest';

import { BalancesTable } from '@/components/BalancesTable';
import { StatusBadge } from '@/components/StatusBadge';

describe('BalancesTable', () => {
  it('shows exact amounts with the quote currency first and sign colors', () => {
    render(
      <BalancesTable
        accounts={[
          { account_id: 'alice', balances: { USD: '-1500.25', AAPL: '10' }, events: 2 },
          { account_id: 'bob', balances: { USD: '1500.25' }, events: 1 },
        ]}
      />,
    );
    const headers = screen.getAllByRole('columnheader').map((h) => h.textContent);
    expect(headers).toEqual(['Account', 'USD', 'AAPL', 'Events']);

    const alice = within(screen.getByRole('row', { name: /alice/ }));
    expect(alice.getByText('-1500.25')).toHaveClass('text-negative');
    expect(alice.getByText('10')).toHaveClass('text-positive');
    const bob = within(screen.getByRole('row', { name: /bob/ }));
    expect(bob.getByText('1500.25')).toBeInTheDocument();
  });

  it('explains the empty state', () => {
    render(<BalancesTable accounts={[]} />);
    expect(screen.getByText(/No balances yet/)).toBeInTheDocument();
  });
});

describe('StatusBadge', () => {
  it.each([
    ['clean', 'Books balanced'],
    ['lagging', 'Cache catching up'],
    ['mismatch', 'Mismatch detected'],
  ] as const)('labels %s', (state, label) => {
    render(
      <StatusBadge
        status={{ state, checked_at: null, accounts: 3, cache_mismatches: 0, lagging: 0 }}
        unreachable={false}
      />,
    );
    expect(screen.getByRole('status')).toHaveTextContent(label);
  });

  it('says when the reconciler cannot be reached', () => {
    render(<StatusBadge status={undefined} unreachable />);
    expect(screen.getByRole('status')).toHaveTextContent('Reconciler unreachable');
  });
});
