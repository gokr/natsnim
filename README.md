# natsnim — a pure-Nim NATS client

A NATS client written in Nim, with **no C dependency**. Nim's only NATS
clients today are FFI bindings to [`nats.c`](https://github.com/nats-io/nats.c)
(`gokr/natswrapper`, `deem0n/nim-nats`), which drag in `libnats`, OpenSSL,
libsodium and protobuf-c. This project removes that.

It is a **translation of the official Go client**
([`nats-io/nats.go`](https://github.com/nats-io/nats.go), Apache-2.0) rather
than of `nats.c` — see [ASSESSMENT.md](ASSESSMENT.md) for the measurements
and the reasoning.

Status: **P1 + P2 landed** — parser, nuid, subject validation, the transport
and the `natswrapper`-compatible shim, all covered by 54 tests. Not yet
ported: reconnect/resubscribe (P5). See the phase plan in
[ASSESSMENT.md](ASSESSMENT.md#plan).

```nim
import nats            # the natswrapper-shaped surface

var nc = connect("nats://127.0.0.1:4222")
defer: nc.close()
var sub: ptr natsSubscription
discard natsConnection_SubscribeSync(addr sub, nc.conn, "ev.>")
var msg: ptr natsMsg
if natsSubscription_NextMsg(addr msg, sub, 1000) == NATS_OK:
  echo $natsMsg_GetSubject(msg), " -> ", $natsMsg_GetData(msg)
  natsMsg_Destroy(msg)
```

The idiomatic API lives in `nats/conn` and is what the shim is built on:

```nim
import nats/conn as natsconn
let c = natsconn.dial("nats://127.0.0.1:4222")
let sub = c.subscribe("ev.>")
c.publish("ev.thing", "hello")
echo sub.nextMsg(1000).data
```

## Scope

In (core NATS):

- connect + `INFO`/`CONNECT` handshake, `PING`/`PONG` (the client answers the
  server's keepalives), `+OK`/`-ERR`
- publish, subscribe/unsubscribe, queue groups
- `MSG`/`HMSG` parsing, wildcard subject matching (server-side; the client
  routes by `sid`, as the protocol intends)
- headers: HMSG is split into `headers` + `data`
- request/reply with inboxes and timeouts, `flush`, `max_payload` enforcement
- optional user/password from the URL

Out (documented rather than half-ported): TLS, nkeys/JWT credentials,
JetStream, KV/object store, micro, WebSocket, compression, automatic
reconnect/backoff. This is a *core NATS, plaintext* client — plenty for a
loopback bus, not a drop-in for a public NATS deployment.

## Threading

**Synchronous: no `asyncdispatch`, no threads, no callbacks.** The socket is
read only inside `nextMsg`/`request`/`flush` via `poll(2)` with the caller's
timeout; the parser demultiplexes into per-subscription queues. `timeout == 0`
is a non-blocking poll, as in `nats.go` and `nats.c`.

Threads would only buy a push/callback API — the thing Niffler's serialized
pump exists to avoid. The trade-off is explicit: **a connection is not safe
for concurrent use from several threads**; a worker thread should own its own
connection. See [ASSESSMENT.md](ASSESSMENT.md#transport-and-threading-contract).

## Tests

```bash
nimble test
```

The parser, nuid, subject and pending-limit tests need nothing else. The two
transport suites start a real `nats-server` and **skip loudly** when none is
found — point `NATS_SERVER_BIN` at one, or put `nats-server` on `PATH`. They
are configured through a generated config file because `max_payload` is a
config-only setting in upstream nats-server.

Beyond upstream's coverage, the suites assert: chunk-boundary invariance of
the parser under randomized splits (including payloads containing `\r\n`,
`PING` and `MSG` bytes, and payloads that *are* those sequences), the two
`-ERR`/`INFO` argument quirks, `nextMsg(0)` not waiting, a message for
subscription B surviving a blocking wait on A, coalesced frames, queue-group
exactly-once delivery, fail-loud pending limits, binary payloads with NUL
bytes, and HMSG from a raw `HPUB` peer.

## Implementation notes

Two Nim details are load-bearing and cost real debugging time; both are
documented at the call site in `src/nats/conn.nim`:

1. **The socket must be unbuffered.** `newSocket()` is buffered, and Nim's
   buffered `recv(fd, size)` loops until it has filled the whole request — so
   asking for 64 KiB after a 6-byte `PONG` blocks forever even though
   `poll(2)` reported readable. This client does its own readiness handling,
   so it uses `newSocket(buffered = false)`.
2. **`connect(..., timeout)` leaves the fd non-blocking.** A later blocking
   `recv` would then fail with `EAGAIN`, which the `SafeDisconn` flag reports
   as `0` = "peer closed". Blocking mode is restored explicitly after connect
   and `poll(2)` decides when a read may happen.

## License

Apache-2.0 — this is a derivative work of `nats-io/nats.go`; see
[LICENSE](LICENSE), [NOTICE](NOTICE) and [PROVENANCE.md](PROVENANCE.md).
