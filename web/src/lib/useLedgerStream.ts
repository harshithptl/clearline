import { useEffect, useRef, useState } from 'react';

import { API, isLedgerEvent, type LedgerEvent } from '@/lib/api';

export type StreamState = 'connecting' | 'live' | 'reconnecting';

/**
 * Subscribes to the balance service's Server-Sent Events stream and calls
 * onEvent for every ledger event. EventSource reconnects by itself; the
 * server drops clients that fall behind, so a reconnect also means "refetch
 * anything you derived" (onReconnect).
 */
export function useLedgerStream(
  onEvent: (ev: LedgerEvent) => void,
  onReconnect?: () => void,
): StreamState {
  const [state, setState] = useState<StreamState>('connecting');
  // Latest callbacks without resubscribing on every render.
  const handlers = useRef({ onEvent, onReconnect });
  useEffect(() => {
    handlers.current = { onEvent, onReconnect };
  });

  useEffect(() => {
    const source = new EventSource(`${API.balance}/events`);
    let opened = false;

    function handleOpen() {
      if (opened) handlers.current.onReconnect?.();
      opened = true;
      setState('live');
    }
    function handleError() {
      setState(opened ? 'reconnecting' : 'connecting');
    }
    function handleLedger(e: MessageEvent<string>) {
      let data: unknown;
      try {
        data = JSON.parse(e.data);
      } catch {
        return;
      }
      if (isLedgerEvent(data)) handlers.current.onEvent(data);
    }

    source.addEventListener('open', handleOpen);
    source.addEventListener('error', handleError);
    source.addEventListener('ledger', handleLedger);
    return () => {
      source.close();
    };
  }, []);

  return state;
}
