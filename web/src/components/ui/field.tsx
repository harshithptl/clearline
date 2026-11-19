import type { ReactNode } from 'react';

import { Label } from '@/components/ui/label';
import { cn } from '@/lib/utils';

interface FieldProps {
  id: string;
  label: string;
  /** Shown under the control; announced with it via aria-describedby. */
  hint?: string;
  error?: string | undefined;
  className?: string;
  children: ReactNode;
}

/** Label + control + hint/error, wired for accessibility. The control must use the given id. */
export function Field({ id, label, hint, error, className, children }: FieldProps) {
  return (
    <div className={cn('grid content-start gap-1.5', className)}>
      <Label htmlFor={id}>{label}</Label>
      {children}
      {error ? (
        <p id={`${id}-desc`} className="text-xs text-negative">
          {error}
        </p>
      ) : hint ? (
        <p id={`${id}-desc`} className="text-xs text-muted-foreground">
          {hint}
        </p>
      ) : null}
    </div>
  );
}
