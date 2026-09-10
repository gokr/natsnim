# nats-nim — a pure-Nim NATS client

A NATS client written in Nim, with **no C dependency**. Nim's only NATS
clients today are FFI bindings to [`nats.c`](https://github.com/nats-io/nats.c)
(`gokr/natswrapper`, `deem0n/nim-nats`), which drag in `libnats`, OpenSSL,
libsodium and protobuf-c. This project removes that.

It is a **translation of the official Go client**
([`nats-io/nats.go`](https://github.com/nats-io/nats.go), Apache-2.0) rather
than of `nats.c` — see [ASSESSMENT.md](ASSESSMENT.md) for the measurements
and the reasoning.

Status: **nothing ported yet** (skeleton + provenance only). See the phase
plan in [ASSESSMENT.md](ASSESSMENT.md#plan).

## Scope

In (v1, core NATS only):

- connect + `INFO`/`CONNECT` handshake, `PING`/`PONG`, `+OK`/`-ERR`
- publish, subscribe/unsubscribe, queue groups
- `MSG`/`HMSG` parsing, wildcard subject matching
- request/reply with inboxes and per-request timeouts
- `NextMsg(timeout)` — a single-threaded, poll-driven API (no callbacks,
  no `asyncdispatch`, no internal threads)
- reconnect + resubscribe, flush, `max_payload`

Out (v1): TLS, nkeys/JWT credentials, JetStream, KV/object store, micro,
WebSocket, compression. This is a *core NATS, plaintext* client — plenty for
a loopback bus, not a drop-in for a public NATS deployment.

## Threading

**Synchronous: no `asyncdispatch`, no threads, no callbacks.** The socket is
read only inside `NextMsg`/`Request`/`Flush` via `select()` with the caller's
timeout; the parser demultiplexes into per-subscription queues. `timeout == 0`
is a non-blocking poll, as in `nats.go` and `nats.c`.

Threads would only buy a push/callback API — the thing Niffler's serialized
pump exists to avoid. The trade-off is explicit: **a connection is not safe
for concurrent use from several threads**; a worker thread should own its own
connection. See [ASSESSMENT.md](ASSESSMENT.md#transport-and-threading-contract).

## Compatibility goal

A drop-in for the `natswrapper` surface used by Niffler, so adoption is a
one-line `requires` change and `sdk/niffler/sdk.nim` does not move. The
exact symbol list is in
[ASSESSMENT.md](ASSESSMENT.md#deliverable-the-shim-contract).

```nim
import nats            # pure Nim

let nc = connect("nats://127.0.0.1:4222")
var sub: ptr natsSubscription
discard natsConnection_SubscribeSync(addr sub, nc.conn, "ev.>")
...
```

## License

Apache-2.0 — this is a derivative work of `nats-io/nats.go`; see
[LICENSE](LICENSE), [NOTICE](NOTICE) and [PROVENANCE.md](PROVENANCE.md).
