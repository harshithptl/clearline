// Exact decimal arithmetic for money in the browser.
//
// The API sends every amount as a decimal string. JavaScript numbers are
// binary floats (0.1 + 0.2 !== 0.3), so amounts are parsed into BigInt
// fixed point with the ledger's scale (NUMERIC(20,8)) and never become a
// Number.

export const SCALE = 8;
const ONE = 10n ** BigInt(SCALE);
const PATTERN = /^(-)?(\d+)(?:\.(\d+))?$/;

/** A decimal as an integer count of 10^-8 units. */
export type Fixed = bigint;

/** Parses "-1500.25" into fixed point. Throws on anything else, or on more than 8 decimals. */
export function parse(s: string): Fixed {
  const m = PATTERN.exec(s.trim());
  if (!m) throw new Error(`not a decimal: ${JSON.stringify(s)}`);
  const [, sign, whole = '0', frac = ''] = m;
  if (frac.length > SCALE) throw new Error(`more than ${SCALE} decimals: ${s}`);
  const v = BigInt(whole) * ONE + BigInt(frac.padEnd(SCALE, '0'));
  return sign ? -v : v;
}

/** Formats fixed point with trailing zeros trimmed ("1500", "-0.5"), like the Go side. */
export function format(v: Fixed): string {
  const neg = v < 0n;
  const abs = neg ? -v : v;
  const whole = (abs / ONE).toString();
  const frac = (abs % ONE).toString().padStart(SCALE, '0').replace(/0+$/, '');
  const body = frac ? `${whole}.${frac}` : whole;
  return neg ? `-${body}` : body;
}

/** Formats with at least minPlaces decimals ("1500.00"), never dropping precision. */
export function formatMin(v: Fixed, minPlaces: number): string {
  const s = format(v);
  const dot = s.indexOf('.');
  const places = dot < 0 ? 0 : s.length - dot - 1;
  if (places >= minPlaces) return s;
  return (dot < 0 ? `${s}.` : s) + '0'.repeat(minPlaces - places);
}

/** a / b, truncated to 8 decimals. b must not be zero. */
export function div(a: Fixed, b: Fixed): Fixed {
  if (b === 0n) throw new Error('division by zero');
  return (a * ONE) / b;
}

export function abs(v: Fixed): Fixed {
  return v < 0n ? -v : v;
}

/** True if s is a positive decimal with at most 8 places. */
export function isPositiveDecimal(s: string): boolean {
  try {
    return parse(s) > 0n;
  } catch {
    return false;
  }
}
