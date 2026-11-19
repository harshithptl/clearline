import type { ComponentProps } from 'react';

import { cn } from '@/lib/utils';

export const fieldControl =
  'h-9 w-full min-w-0 rounded-md border border-input bg-background px-3 text-sm shadow-xs transition-colors placeholder:text-muted-foreground focus-visible:ring-2 focus-visible:ring-ring focus-visible:outline-none disabled:cursor-not-allowed disabled:opacity-50 aria-invalid:border-negative';

export function Input({ className, ...props }: ComponentProps<'input'>) {
  return <input className={cn(fieldControl, className)} {...props} />;
}
