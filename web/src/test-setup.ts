import '@testing-library/jest-dom/vitest';
import { cleanup } from '@testing-library/react';
import { afterEach } from 'vitest';

// jsdom lacks a few layout and pointer APIs that Radix primitives call. The
// DOM types declare them as always present, so check at runtime and install
// no-op stand-ins: tests assert behavior, not layout.
class ResizeObserverStub {
  observe() {}
  unobserve() {}
  disconnect() {}
}

function polyfill(target: object, name: string, value: unknown) {
  if (!(name in target)) {
    Object.defineProperty(target, name, { value, writable: true, configurable: true });
  }
}

polyfill(globalThis, 'ResizeObserver', ResizeObserverStub);
polyfill(Element.prototype, 'hasPointerCapture', () => false);
polyfill(Element.prototype, 'releasePointerCapture', () => undefined);
polyfill(Element.prototype, 'scrollIntoView', () => undefined);

afterEach(() => {
  cleanup();
});
