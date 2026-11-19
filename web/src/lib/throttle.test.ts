import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import { throttle } from '@/lib/throttle';

describe('throttle', () => {
  beforeEach(() => {
    vi.useFakeTimers();
  });
  afterEach(() => {
    vi.useRealTimers();
  });

  it('runs once per window however many calls arrive', () => {
    const fn = vi.fn();
    const t = throttle(fn, 1000);
    for (let i = 0; i < 500; i++) t.call(); // a burst of ledger events
    expect(fn).not.toHaveBeenCalled();
    vi.advanceTimersByTime(1000);
    expect(fn).toHaveBeenCalledTimes(1);

    t.call();
    vi.advanceTimersByTime(999);
    expect(fn).toHaveBeenCalledTimes(1);
    vi.advanceTimersByTime(1);
    expect(fn).toHaveBeenCalledTimes(2);
  });

  it('cancel drops the pending run', () => {
    const fn = vi.fn();
    const t = throttle(fn, 1000);
    t.call();
    t.cancel();
    vi.advanceTimersByTime(5000);
    expect(fn).not.toHaveBeenCalled();
  });
});
