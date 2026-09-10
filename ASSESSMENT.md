# Assessment — a pure-Nim NATS client

Status: **decision record + port plan. Nothing ported yet.**

## The problem

Nim has no pure-Nim NATS client. Every available option is a binding to the C
client (`nats.c`), so every Nim NATS program inherits a native dependency
chain (`libnats`, `libssl`, `libsodium`/nkeys, `protobuf-c`, `libuv`):

| package | what it is |
|---|---|
| `gokr/natswrapper` (used by Niffler) | Futhark FFI over `nats.h`, `{.passL: "-lnats".}` |
| `deem0n/nim-nats` (the only NATS package in nimble's official index) | also an FFI over `nats.c` |

That is the last host prerequisite standing between a fresh clone and
`nimble install && run` for the Nim half of Niffler. Goal: a **pure-Nim**
client with no C dependency that is a **drop-in for the `natswrapper` surface
Niffler actually uses** (18 symbols, listed below) — not a redesign of the
SDK.

## Candidate sources (measured, shallow clones)

| | **nats.go** | **nats.c** |
|---|---|---|
| pinned commit | `1ffb90b` (2026-09-10) | `9cae373` (2026-09-08) |
| license | Apache-2.0 | Apache-2.0 |
| non-test source | **37,996 lines** / 128 files | **59,108 lines** (55 `.c` + headers) |
| core client | `nats.go` 6,867 + `parser.go` 555 | `conn.c` 5,023 + `util.c` 2,865 + `opts.c` 1,910 + `sub.c` 1,378 + `msg.c` 968 + `parser.c` 940 |
| tests | **58,077 lines** (1.5× the code), incl. a dedicated parser suite | C test harness |
| deps | `nkeys`, `nuid`, `klauspost/compress`, `x/crypto`, `x/sys` | OpenSSL (TLS), pthreads, optional libsodium, libuv/libevent for async |
| memory model | tracing GC | manual refcounts (`natsMsg_Destroy`, buffers) |
| concurrency | goroutines + mutexes + channels | internal reader thread + spin options |
| not needed (JetStream/KV/obj/micro) | `js.go` 4,200 + `jsm.go` 1,798 + `jetstream/*` ~10k | `js.c` 4,195 + `jsm.c` 4,538 + `object.c`/`kv.c` ~4,400 |

## The call: **translate nats.go, not nats.c**

1. **Semantic distance is much smaller.** Nim and Go share a tracing GC,
   value strings, slices/`seq`, reference objects, `defer`, and errors as
   values. C forces the port to reproduce ownership and out-params
   (`natsStatus *`, caller-owned buffers, explicit destroy) — i.e. we would
   be hand-writing, in Nim, exactly the memory discipline Nim exists to
   avoid. The instinct that "C is closer because both are native" is the
   trap: compile target is irrelevant, *ownership model* is everything.
2. **The parser is the crown jewel and it is self-contained.** `parser.go`
   is a 555-line byte-oriented state machine with its own internal package
   and its own tests; `nats.c`'s `parser.c` is welded to its `natsBuffer`
   refcount layer.
3. **nats.c's concurrency is the wrong shape and would be discarded anyway.**
   It owns a reader thread plus spin options. Niffler's SDK is a
   single-threaded polled pump (`NextMsg(timeout)`, no callbacks, no
   `asyncdispatch`). We must redesign the transport either way — and doing
   that from Go's explicit state + callbacks is far easier to read than
   from C's hidden thread.
4. **Tests are the porting oracle.** 58k lines of Go tests, including a
   parser suite that translates directly into Nim `unittest`/`check` cases.
   nats.c's harness is heavier to stand up per-case.
5. **Smaller core.** The part we need is ~7.4k lines of Go (`nats.go` +
   `parser.go`) vs ~13k lines of C spread across six files with a runtime
   dependency on OpenSSL.

Honest counterpoint: nats.c has been the substrate for *all* non-Go
ecosystems, its behavior is the de-facto cross-language spec, and it is the
thing we currently link against — so for "match current behavior exactly",
nats.c is the reference. But it is the reference for *behavior*, not for
*translation source*. Keep it as the differential oracle (Phase 6), not as
the file-by-file origin.

## What "automatic translation" can and cannot cover

Being precise, because this determines the effort:

- **Mechanical (~1,200–1,500 lines):** `parser.go` (byte state machine, all
  the `MSG`/`HMSG`/`INFO`/`PING`/`PONG`/`+OK`/`-ERR` handling), `nuid`
  (unique ids for inboxes), subject/wildcard matching, inbox generation,
  status/error taxonomy and their strings, the `CONNECT` JSON, reconnect
  policy constants. These can be translated file-by-file with a human/agent
  review pass and gated entirely by *translated* upstream tests.
- **Not mechanical (~1,500–2,000 lines):** the transport. Go's goroutine
  read loop + channels + `sync.Mutex` must be *replaced* by a
  single-threaded, poll-based socket loop whose blocking points are the
  caller's `NextMsg(timeout)` / `Request(timeout)`. That is a design task,
  and it is *simpler* than either original — it is exactly what Niffler's
  pump wants. Port it as the same state machine with a different driver.
- **Out of scope for v1:** TLS, nkeys/JWT credentials, JetStream, KV/object
  store, micro, WebSocket, compression. Document them as unsupported rather
  than half-porting them. Niffler is loopback-only with no auth by design
  ("the child holds no credentials"), so none of these are on the critical
  path.

## Deliverable: the shim contract

The library must be a drop-in for `natswrapper`, because that is what makes
adoption a one-line `requires` change. The full surface the Nim sources use
today (`sdk/niffler/`, `core/`, components):

```
nats_Open                                     natsMsg_GetData
connect(url) -> Connection                    natsMsg_GetDataLength
Connection.publish(subject, string)           natsMsg_GetSubject
natsConnection_SubscribeSync                  natsMsg_GetReply
natsConnection_QueueSubscribeSync             natsMsg_Destroy
natsConnection_Request                        natsSubscription_NextMsg
natsConnection_PublishRequest                 natsSubscription_Unsubscribe
natsConnection_FlushTimeout                   natsSubscription_Destroy
natsConnection_GetMaxPayload                  checkStatus / NATS_OK / NATS_TIMEOUT
```

Keeping these names (a compatibility module) means `sdk/niffler/sdk.nim` and
the core do not change at all.

## Plan

| phase | content | gate |
|---|---|---|
| **P0** | this repo: license, NOTICE, provenance map, package skeleton | – |
| **P1** | `parser.nim` + `nuid.nim` + subject matching, translated from `parser.go` | translated upstream parser tests pass |
| **P2** | transport + connection: connect handshake, `pub`/`sub(unsub)`, `NextMsg(timeout)`, `flush`, `close`, max_payload | `t_bus.nim` against `components/nats` |
| **P3** | request/reply: inboxes, `PublishRequest`, `Request(timeout)` | `t_request.nim`; Niffler's `requestEnvelope` path |
| **P4** | queue groups, `Unsubscribe`, error/status fidelity | `t_queuegroup.nim` |
| **P5** | reconnect + resubscribe (the subtle one) | ported upstream reconnect tests + a kill-the-server test |
| **P6** | differential harness: same operations through this client and through `natswrapper`, assert identical observable behavior | recorded traces equal |
| **P7** | Niffler integration behind a flag; then flip the default and drop libnats | `make test` green, `make doctor` no longer needs libnats |

## Acceptance

- All Niffler Nim components build and run with `requires
  "https://github.com/gokr/nats-nim"` and **no `libnats`** on the box.
- `make test` green (smoke, bash, store, core, agent, fabric, mcp, …).
- Differential traces identical for the used surface.
- `make doctor` / `make setup` no longer install `libnats-dev` / `cnats`.

## Risks

- **Reconnect/resubscribe** is where clients grow bugs; loopback tests rarely
  exercise it. Mitigation: translate the upstream tests first, then add a
  server-restart test.
- **TLS + nkeys/JWT unsupported in v1.** Fine for Niffler, but say so loudly
  in the README so nobody adopts it for a public NATS deployment.
- **License/provenance.** Apache-2.0 derivative: keep `LICENSE`, add
  `NOTICE`, and keep `PROVENANCE.md` mapping each Nim module to its upstream
  file + commit.
- **Name collision.** `nim-nats` (nimble index) is a `nats.c` wrapper; pick a
  distinct package name so `nimble install` is unambiguous.
