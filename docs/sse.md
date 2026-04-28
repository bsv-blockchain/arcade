# Integrating with the `/events` SSE endpoint

Arcade exposes a [Server-Sent Events](https://html.spec.whatwg.org/multipage/server-sent-events.html) stream at `GET /events` that pushes a frame every time a tracked transaction changes status. This document covers the wire format, how to scope the stream to your own transactions, and how reconnection / replay works.

## Endpoint

```
GET /events
```

| Param | Where | Purpose |
|---|---|---|
| `callbackToken` | query string | Filters the stream to txids that were submitted under this token. Omit to receive every status update the server sees. |
| `Last-Event-ID` | request header | Nanosecond timestamp from a prior `id:` line. On reconnect, the server replays updates that occurred after this timestamp. **Only takes effect when `callbackToken` is also set** — without a token there is no way to scope replay to your txids. |

Response is `Content-Type: text/event-stream` and stays open until either side disconnects.

## Submitting transactions under a token

Pick any opaque string as your token (`crypto.randomUUID()` works) and send it as a header on every submit:

```bash
curl -X POST https://arcade.example.com/tx \
  -H "Content-Type: application/octet-stream" \
  -H "X-CallbackToken: my-session-abc123" \
  --data-binary @tx.bin
```

The server records a submission row keyed by `(txid, callbackToken)`. The same token then scopes the SSE stream:

```
GET /events?callbackToken=my-session-abc123
```

You may also set `X-CallbackUrl` to additionally receive status updates as outbound webhooks; that is independent of SSE.

## Frame format

```
id: 1745870456123456789
event: status
data: {"txid":"abc...","txStatus":"SEEN_ON_NETWORK","timestamp":"2026-04-28T18:20:56Z"}

```

- `id` is a Unix nanosecond timestamp. Clients should remember the most recent `id` and send it back as `Last-Event-ID` on reconnect (browser `EventSource` does this automatically).
- `event: status` is the only event type emitted for transaction updates.
- `data` is one JSON object per frame with three fields: `txid`, `txStatus`, `timestamp` (RFC 3339).
- A blank line terminates each frame.

Every ~15 seconds the server emits a `: keepalive` comment frame so idle proxies don't kill the connection. Comment frames have no `event:` and should be ignored by clients.

## Browser (`EventSource`)

```js
const es = new EventSource(
  'https://arcade.example.com/events?callbackToken=my-session-abc123'
);

es.addEventListener('status', (ev) => {
  const { txid, txStatus, timestamp } = JSON.parse(ev.data);
  console.log(txid, '→', txStatus, '@', timestamp);
});

es.onerror = (err) => console.error('SSE error', err);
```

`EventSource` reconnects automatically and sends the most recent `id` it observed back as `Last-Event-ID`, so dropped connections replay missed updates without any extra code (provided the original request supplied a `callbackToken`).

`EventSource` does not allow custom headers, so authentication / token scoping must be in the URL — which is exactly what the `callbackToken` query string is for.

## Node.js / non-browser clients

`EventSource` is a browser API. From Node.js or other backends, either:

- use a polyfill like [`eventsource`](https://www.npmjs.com/package/eventsource) on npm, or
- read the response stream directly:

```ts
const res = await fetch('https://arcade.example.com/events?callbackToken=my-session-abc123', {
  headers: { 'Last-Event-ID': lastSeenID ?? '' },
});

const reader = res.body!.getReader();
const decoder = new TextDecoder();
let buf = '';
for (;;) {
  const { value, done } = await reader.read();
  if (done) break;
  buf += decoder.decode(value, { stream: true });
  let sep;
  while ((sep = buf.indexOf('\n\n')) !== -1) {
    const frame = buf.slice(0, sep);
    buf = buf.slice(sep + 2);
    handleFrame(frame); // parse `id:` / `event:` / `data:` lines
  }
}
```

When implementing manually, remember to:

- Track the most recent `id:` line and resend it as `Last-Event-ID` on reconnect.
- Reconnect with backoff on transport errors.
- Discard `: keepalive` comments.

## `curl` for sanity checks

```bash
curl -N "https://arcade.example.com/events?callbackToken=my-session-abc123"
```

`-N` disables curl's output buffering so you see frames live as they arrive.

## Reconnection and replay

The server only replays missed events when **both** of these are present on the reconnect:

- `?callbackToken=<token>` query string
- `Last-Event-ID: <ns-timestamp>` header

Replay walks the submissions registered under the token and emits the current status for each txid whose `timestamp` is later than `Last-Event-ID`. There is no replay for unfiltered (no-token) consumers — that mode is for live monitoring only.

Slow consumers will drop frames: the fan-out is non-blocking, so if your client doesn't drain the connection fast enough, the server skips that update for that client rather than stalling. Reconnecting with `Last-Event-ID` is the recovery path.

## Failure modes

| Status | Body | Meaning |
|---|---|---|
| `503` | `{"error":"events stream not enabled"}` | The deployment didn't wire a publisher. Nothing to subscribe to. |
| `500` | `{"error":"streaming unsupported"}` | Response writer doesn't support flushing. Shouldn't happen behind a normal HTTP server. |

## End-to-end flow

1. Generate a `callbackToken` once per session (e.g. `crypto.randomUUID()`).
2. Open `GET /events?callbackToken=<token>` and start handling `event: status` frames.
3. Submit transactions via `POST /tx` (or `POST /txs`) with `X-CallbackToken: <token>`.
4. Each submitted txid will produce a sequence of status frames on the open stream as it propagates and gets mined.
5. On reconnect, your client (or `EventSource`) sends `Last-Event-ID` automatically, and the server fills in any updates you missed.
