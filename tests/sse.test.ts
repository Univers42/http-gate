import assert from 'node:assert/strict';
import test from 'node:test';

import { openSseStream, parseSseBlock, pumpSseStream, type StreamPayload } from '../src/sse.ts';
import type { FetchLike } from '../src/client.ts';

/** Build a Response whose body streams `chunks` in order. */
function streamResponse(chunks: string[], init?: ResponseInit): Response {
  const encoder = new TextEncoder();
  const body = new ReadableStream<Uint8Array>({
    start(controller) {
      for (const chunk of chunks) controller.enqueue(encoder.encode(chunk));
      controller.close();
    },
  });
  return new Response(body, init);
}

async function collect(response: Response): Promise<Array<[string, StreamPayload]>> {
  const events: Array<[string, StreamPayload]> = [];
  await pumpSseStream(response, (event, data) => {
    events.push([event, data]);
  });
  return events;
}

test('parses an event/data pair', () => {
  assert.deepEqual(parseSseBlock('event: token\ndata: {"text":"hi"}'), {
    event: 'token',
    data: { text: 'hi' },
  });
});

test('defaults the event name to "message" when absent', () => {
  assert.deepEqual(parseSseBlock('data: {"text":"hi"}'), { event: 'message', data: { text: 'hi' } });
});

test('joins multi-line data before parsing', () => {
  assert.deepEqual(parseSseBlock('data: {"text":\ndata: "wrapped"}'), {
    event: 'message',
    data: { text: 'wrapped' },
  });
});

test('returns null for a block with no data or invalid JSON', () => {
  assert.equal(parseSseBlock(''), null);
  assert.equal(parseSseBlock('event: ping'), null, 'a data-less block carries no payload');
  assert.equal(parseSseBlock('data: not-json'), null, 'a malformed block is skipped, not thrown');
});

test('parses the frame id when the block carries one', () => {
  assert.deepEqual(parseSseBlock('id: 42\nevent: token\ndata: {"text":"hi"}'), {
    event: 'token',
    data: { text: 'hi' },
    id: '42',
  });
});

test('leaves the id out when the block carries none', () => {
  const parsed = parseSseBlock('event: token\ndata: {"text":"hi"}');
  assert.ok(parsed);
  assert.equal(parsed.id, undefined, 'no id at all');
  assert.deepEqual(Object.keys(parsed).sort(), ['data', 'event'], 'not even an id key — an old caller sees the same shape');
});

test('trims the id and ignores an empty one', () => {
  assert.equal(parseSseBlock('id:  7 \ndata: {}')?.id, '7');
  assert.equal(parseSseBlock('id:\ndata: {}')?.id, undefined, 'an empty id is no id');
});

test('pump hands the frame id to the handler', async () => {
  const seen: Array<[string, string | undefined]> = [];
  await pumpSseStream(
    streamResponse(['id: 1\nevent: a\ndata: {"n":1}\n\n', 'event: b\ndata: {"n":2}\n\n']),
    (event, _data, id) => {
      seen.push([event, id]);
    },
  );
  assert.deepEqual(seen, [
    ['a', '1'],
    ['b', undefined],
  ]);
});

test('pump emits each complete block in order', async () => {
  const events = await collect(
    streamResponse(['event: a\ndata: {"n":1}\n\n', 'event: b\ndata: {"n":2}\n\n']),
  );
  assert.deepEqual(events, [
    ['a', { n: 1 }],
    ['b', { n: 2 }],
  ]);
});

test('pump reassembles a block split across chunks', async () => {
  // The split lands mid-JSON — the decoder must buffer rather than parse early.
  const events = await collect(streamResponse(['event: a\ndata: {"n":', '1}\n\n']));
  assert.deepEqual(events, [['a', { n: 1 }]]);
});

test('pump flushes a trailing block with no terminator', async () => {
  const events = await collect(streamResponse(['data: {"n":1}\n\n', 'data: {"n":2}']));
  assert.deepEqual(events.at(-1), ['message', { n: 2 }], 'the final block must not be dropped');
});

test('pump awaits an async handler before the next event', async () => {
  const order: string[] = [];
  await pumpSseStream(streamResponse(['data: {"n":1}\n\n', 'data: {"n":2}\n\n']), async (_event, data) => {
    order.push(`start:${data.n}`);
    await new Promise((resolve) => setTimeout(resolve, 1));
    order.push(`end:${data.n}`);
  });
  assert.deepEqual(order, ['start:1', 'end:1', 'start:2', 'end:2']);
});

test('pump rejects a response with no body', async () => {
  await assert.rejects(pumpSseStream(new Response(null, { status: 204 }), () => {}), /no body/);
});

test('open returns the first origin that answers', async () => {
  const tried: string[] = [];
  const impl: FetchLike = async (url) => {
    tried.push(url);
    if (url.startsWith('http://down')) return new Response('nope', { status: 503 });
    return streamResponse(['data: {}\n\n']);
  };

  const response = await openSseStream(['http://down', 'http://up'], '/chat', {}, impl);
  assert.ok(response.ok);
  assert.deepEqual(tried, ['http://down/chat', 'http://up/chat']);
});

test('open skips an origin that throws and continues down the list', async () => {
  const impl: FetchLike = async (url) => {
    if (url.startsWith('http://broken')) throw new Error('DNS failure');
    return streamResponse(['data: {}\n\n']);
  };
  const response = await openSseStream(['http://broken', 'http://up'], '/chat', {}, impl);
  assert.ok(response.ok);
});

test('open reports the last failure when every origin is exhausted', async () => {
  const impl: FetchLike = async () => new Response('nope', { status: 500 });
  await assert.rejects(openSseStream(['http://a', 'http://b'], '/chat', {}, impl), /http:\/\/b returned 500/);
});

test('open rejects when given no origins at all', async () => {
  const impl: FetchLike = async () => streamResponse(['data: {}\n\n']);
  await assert.rejects(openSseStream([], '/chat', {}, impl), /No SSE origin is available\./);
});

test('open POSTs the body with SSE headers', async () => {
  let seen: RequestInit | undefined;
  const impl: FetchLike = async (_url, init) => {
    seen = init;
    return streamResponse(['data: {}\n\n']);
  };
  await openSseStream(['http://up'], '/chat', { prompt: 'hi' }, impl);

  assert.equal(seen?.method, 'POST');
  assert.equal((seen?.headers as Record<string, string>)['Accept'], 'text/event-stream');
  assert.equal(seen?.body, '{"prompt":"hi"}');
});

test('open GETs without a body and carries the extra headers', async () => {
  let seen: RequestInit | undefined;
  const impl: FetchLike = async (_url, init) => {
    seen = init;
    return streamResponse(['data: {}\n\n']);
  };
  await openSseStream(['http://up'], '/runs/abc/events', undefined, impl, {
    method: 'GET',
    headers: { 'Last-Event-ID': '3' },
  });

  assert.equal(seen?.method, 'GET');
  assert.equal(seen?.body, undefined, 'a GET stream sends no body');
  assert.equal((seen?.headers as Record<string, string>)['Last-Event-ID'], '3');
  assert.equal((seen?.headers as Record<string, string>)['Content-Type'], undefined, 'no body, no content type');
});

test('open passes the abort signal to the request', async () => {
  const controller = new AbortController();
  let seen: RequestInit | undefined;
  const impl: FetchLike = async (_url, init) => {
    seen = init;
    return streamResponse(['data: {}\n\n']);
  };
  await openSseStream(['http://up'], '/chat', {}, impl, { signal: controller.signal });

  assert.equal(seen?.signal, controller.signal);
});

test('open stops at the first origin when the request is aborted', async () => {
  const controller = new AbortController();
  const tried: string[] = [];
  const impl: FetchLike = async (url) => {
    tried.push(url);
    controller.abort();
    throw new DOMException('The operation was aborted.', 'AbortError');
  };

  await assert.rejects(
    openSseStream(['http://a', 'http://b'], '/chat', {}, impl, { signal: controller.signal }),
    /aborted/i,
  );
  assert.deepEqual(tried, ['http://a/chat'], 'an abort is not a failed origin — the list is not walked');
});

