import * as RadioGroupPrimitive from '@radix-ui/react-radio-group';
import type { ComponentProps } from 'react';

import { cn } from '@/lib/utils';

/**
 * A row of mutually exclusive options (buy / sell), built on Radix RadioGroup:
 * arrow keys move between options and screen readers announce a radio group.
 */
export function Segmented({
  className,
  ...props
}: ComponentProps<typeof RadioGroupPrimitive.Root>) {
  return (
    <RadioGroupPrimitive.Root
      className={cn(
        'inline-flex h-9 w-full rounded-md border border-input bg-muted p-0.5',
        className,
      )}
      {...props}
    />
  );
}

export function SegmentedItem({
  className,
  ...props
}: ComponentProps<typeof RadioGroupPrimitive.Item>) {
  return (
    <RadioGroupPrimitive.Item
      className={cn(
        'flex-1 rounded-sm text-sm font-medium text-muted-foreground capitalize transition-colors focus-visible:ring-2 focus-visible:ring-ring focus-visible:outline-none data-[state=checked]:bg-card data-[state=checked]:text-foreground data-[state=checked]:shadow-xs',
        className,
      )}
      {...props}
    />
  );
}
