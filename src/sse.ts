/**
 * Server-Sent Events: parse, open, pump. No framework, no ambient config —
 * the React binding lives in `./react` so this module stays loadable by
 * runtimes that only strip types (no JSX, no DOM globals beyond fetch).
 */

import type { FetchLike } from './client.ts';

/** Arbitrary JSON payload, with the fields SSE agents conventionally send. */
export type StreamPayload = Record<string, unknown> & {
  text?: string;
  id?: string;
  name?: string;
  input?: unknown;
  toolUseId?: string;
  message?: string;
};

/**
 * Receives every decoded event. May be async — the pump awaits it in order.
 * The third argument is the frame's `id:` field, when the frame carries one.
 */
export type StreamEventHandler = (event: string, data: StreamPayload, id?: string) => Promise<void> | void;

/** One decoded SSE block. `id` is present only when the frame carried an `id:` field. */
export interface SseBlock {
  event: string;
  data: StreamPayload;
  /** The frame's `id:` field — the cursor a reconnect resumes after. */
  id?: string;
}

/**
 * Parse one `event:` / `data:` / `id:` block. Returns `null` when the block
 * carries no data or the payload is not JSON — a partial flush is normal
 * mid-stream, so a malformed block is skipped rather than thrown.
 *
 * The `id` is only a key when the block has one: a frame without an `id:`
 * line parses exactly as it did before, so existing callers are untouched.
 */
export function parseSseBlock(block: string): SseBlock | null {
  const lines = block.split('\n');
  const eventLine = lines.find((line) => line.startsWith('event:'));
  const idLine = lines.find((line) => line.startsWith('id:'));
  const dataLines = lines.filter((line) => line.startsWith('data:')).map((line) => line.slice(5).trimStart());
  if (dataLines.length === 0) return null;
  try {
    const parsed: SseBlock = {
      event: eventLine?.slice(6).trim() || 'message',
      data: JSON.parse(dataLines.join('\n')) as StreamPayload,
    };
    const id = idLine?.slice(3).trim();
    if (id) parsed.id = id;
    return parsed;
  } catch {
    return null;
  }
}

/** Extra knobs for a stream request. Everything here is optional. */
export interface SseStreamOptions {
  /** Aborts the request — and the reader — when it fires. */
  signal?: AbortSignal;
  /** Extra headers, merged over the SSE defaults (e.g. `Last-Event-ID`). */
  headers?: Record<string, string>;
  /** `GET` sends no body; `POST` sends the JSON body. Defaults to `POST`. */
  method?: 'GET' | 'POST';
}

/**
 * POST to each origin in turn and return the first streaming response. Throws
 * the last failure when every origin is exhausted.
 *
 * `options` is additive: without it the call behaves exactly as before — a
 * POST with a JSON body and the SSE headers.
 */
export async function openSseStream(
  origins: readonly string[],
  endpoint: string,
  body: unknown,
  fetchImpl?: FetchLike,
  options?: SseStreamOptions,
): Promise<Response> {
  const doFetch: FetchLike = fetchImpl ?? ((input, init) => fetch(input, init));
  const method = options?.method ?? 'POST';
  let lastError: Error | null = null;

  for (const origin of origins) {
    try {
      const response = await doFetch(`${origin}${endpoint}`, {
        method,
        headers: {
          Accept: 'text/event-stream',
          ...(method === 'POST' ? { 'Content-Type': 'application/json' } : {}),
          ...(options?.headers ?? {}),
        },
        ...(method === 'POST' ? { body: JSON.stringify(body) } : {}),
        ...(options?.signal ? { signal: options.signal } : {}),
      });
      if (response.ok && response.body) return response;
      lastError = new Error(`SSE endpoint ${origin} returned ${response.status}`);
    } catch (error) {
      // An aborted request is not a failed origin: stop instead of walking the list.
      if (options?.signal?.aborted) throw error;
      lastError = error instanceof Error ? error : new Error(`SSE endpoint ${origin} failed.`);
    }
  }
  throw lastError ?? new Error('No SSE origin is available.');
}

/**
 * Drain `response.body`, splitting on the blank-line block separator and
 * relaying each parsed block to `onEvent`. The trailing buffer is flushed once
 * the stream ends, so a final block without a terminator is not lost.
 */
export async function pumpSseStream(response: Response, onEvent: StreamEventHandler): Promise<void> {
  const body = response.body;
  if (!body) throw new Error('SSE response has no body to read.');

  const reader = body.getReader();
  const decoder = new TextDecoder();
  let buffer = '';

  for (;;) {
    const { value, done } = await reader.read();
    if (done) break;
    buffer += decoder.decode(value, { stream: true });
    const blocks = buffer.split('\n\n');
    buffer = blocks.pop() ?? '';
    for (const block of blocks) {
      const parsed = parseSseBlock(block);
      if (parsed) await onEvent(parsed.event, parsed.data, parsed.id);
    }
  }

  const parsed = parseSseBlock(buffer);
  if (parsed) await onEvent(parsed.event, parsed.data, parsed.id);
}
