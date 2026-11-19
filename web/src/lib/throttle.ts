/**
 * Coalesces bursts of calls into at most one run of fn per `ms`, on the
 * trailing edge. The ledger stream can deliver hundreds of events a second
 * under load; refetching balances for each one turned the dashboard into a
 * request storm against the balance service. One refetch per window carries
 * every change in that window.
 */
export interface Throttle {
  call: () => void;
  cancel: () => void;
}

export function throttle(fn: () => void, ms: number): Throttle {
  let timer: ReturnType<typeof setTimeout> | null = null;
  return {
    call() {
      if (timer !== null) return;
      timer = setTimeout(() => {
        timer = null;
        fn();
      }, ms);
    },
    cancel() {
      if (timer !== null) clearTimeout(timer);
      timer = null;
    },
  };
}
