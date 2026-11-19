import { AlertTriangle, CheckCircle2, CircleDashed, Loader2 } from 'lucide-react';
import type { ReactNode } from 'react';

import { Badge, type BadgeProps } from '@/components/ui/badge';
import type { ReconcilerState, ReconcilerStatus } from '@/lib/api';

const look: Record<ReconcilerState, { label: string; tone: BadgeProps['tone']; icon: ReactNode }> =
  {
    pending: { label: 'Reconciler starting', tone: 'warning', icon: <Loader2 /> },
    clean: { label: 'Books balanced', tone: 'positive', icon: <CheckCircle2 /> },
    lagging: { label: 'Cache catching up', tone: 'warning', icon: <Loader2 /> },
    mismatch: { label: 'Mismatch detected', tone: 'negative', icon: <AlertTriangle /> },
    error: { label: 'Reconciler error', tone: 'negative', icon: <AlertTriangle /> },
  };

interface Props {
  status: ReconcilerStatus | undefined;
  unreachable: boolean;
}

/** The reconciler's latest verdict on the SPEC 4.2 invariants. */
export function StatusBadge({ status, unreachable }: Props) {
  if (unreachable || !status) {
    return (
      <Badge tone="negative" role="status">
        <CircleDashed aria-hidden="true" />
        Reconciler unreachable
      </Badge>
    );
  }
  const { label, tone, icon } = look[status.state];
  const checked = status.checked_at ? new Date(status.checked_at).toLocaleTimeString() : 'never';
  return (
    <Badge tone={tone} role="status" title={`${status.accounts} accounts, checked ${checked}`}>
      <span aria-hidden="true" className="contents">
        {icon}
      </span>
      {label}
    </Badge>
  );
}
