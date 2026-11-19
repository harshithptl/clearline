import { describe, expect, it } from 'vitest';

import { div, format, formatMin, isPositiveDecimal, parse } from '@/lib/decimal';

describe('decimal', () => {
  it('round-trips exactly where floats would not', () => {
    // 0.1 + 0.2 is 0.30000000000000004 as a float.
    expect(format(parse('0.1') + parse('0.2'))).toBe('0.3');
    expect(format(parse('-1500.25000000'))).toBe('-1500.25');
    expect(format(parse('12345678901.12345678'))).toBe('12345678901.12345678');
    expect(format(parse('0'))).toBe('0');
    expect(format(parse('-0.5'))).toBe('-0.5');
  });

  it('rejects non-decimals and excess precision', () => {
    for (const bad of ['', 'abc', '1e5', '1.2.3', '1.123456789', '--1', '0x10']) {
      expect(() => parse(bad), bad).toThrow();
    }
  });

  it('divides notional by quantity into a price', () => {
    expect(format(div(parse('501.25'), parse('5')))).toBe('100.25');
    expect(format(div(parse('1'), parse('3')))).toBe('0.33333333'); // truncated at 8 places
    expect(() => div(parse('1'), 0n)).toThrow();
  });

  it('pads to a minimum number of places without losing any', () => {
    expect(formatMin(parse('1500'), 2)).toBe('1500.00');
    expect(formatMin(parse('1.5'), 2)).toBe('1.50');
    expect(formatMin(parse('1.125'), 2)).toBe('1.125');
  });

  it('checks positive decimals', () => {
    expect(isPositiveDecimal('150.25')).toBe(true);
    expect(isPositiveDecimal('0')).toBe(false);
    expect(isPositiveDecimal('-1')).toBe(false);
    expect(isPositiveDecimal('nope')).toBe(false);
  });
});
