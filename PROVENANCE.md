# Provenance

This library is a Nim translation of the official Go client and its helpers:

- **Source:** https://github.com/nats-io/nats.go
- **Pinned commit:** `1ffb90b` (2026-09-10)
- **Source:** https://github.com/nats-io/nuid (vendored by nats.go)
- **Pinned version:** `v1.0.1`
- **License:** Apache-2.0 (see [LICENSE](LICENSE), [NOTICE](NOTICE))

Per-file map. Each ported Nim file carries a header naming its upstream
origin, and this table is the authoritative list.

| Nim | upstream | notes |
|---|---|---|
| `src/nats/parser.nim` | `parser.go` | state machine, `MsgArgs`, `parseInt64`. Deviations (documented in the file): no `scratch`/`argBuf`/`msgBuf` reuse, so no `cloneMsgArg`; the payload is always copied; no statistics (they belong to the connection). State enum keeps Go's iota order. |
| `src/nats/nuid.nim` | `nuid` `nuid.go` @ v1.0.1 | per-instance sequential PRNG instead of Go's globally seeded `math/rand`; the process-global generator is **not** mutex-guarded (no threads by design); `newNuID(seed)` is exposed for tests. |
| `src/nats/subject.nim` | `nats.go` (`badSubject`, `badQueue`) | no client-side wildcard matching exists to port: the server routes by sid. |
| `tests/t_parser.nim` | `nats_test.go` (`TestParserPing`, `TestParserErr`, `TestParserOK`, `TestParserShouldFail`, `TestParserSplitMsg`) | plus INFO and HMSG cases. Upstream's `argBuf != nil` assertions are Go buffer-lifetime artifacts and become hasArgBuf checks (see the note in the test). |
| `tests/t_nuid.nim` | `nuid` `nuid_test.go`, `unique_test.go` | uniqueness reduced from 10M to 200k iterations; the seed-determinism case asserts the sequential tail only (the prefix is crypto-random upstream too). |
| `tests/t_subject.nim` | – | derived from the `badSubject`/`badQueue` semantics; upstream has no dedicated test. |
| `src/nats/conn.nim` | `nats.go` (`Conn`: connect, publish, subscribe, request, flush) | **transport rewritten, not translated.** Go's reader goroutine + channels + mutexes are replaced by a single-threaded poll(2)-driven socket. Deviations: RSS of *one* connection, no background thread; `unsubscribe` discards pending (Go's `Unsubscribe`); overrun of the pending limit is recorded and raised by the next `nextMsg` instead of going to an async error callback; no TLS, nkeys/JWT, WebSocket or reconnect. |
| `src/nats.nim` | `gokr/natswrapper` over `nats.c` (its public shape) | the shim: identical names/signatures to the binding Niffler uses, so adoption is a `requires` change. Adds `natsConnection_Publish` (data + length, binary-safe) and `natsMsg_GetHeader`, which `natswrapper` does not surface. Handles are GC-managed, so `*Destroy` detaches rather than frees. |
| `tests/t_bus.nim` | `nats_test.go` (in part) | plus the properties upstream has no counterpart for, because its reader goroutine solves them implicitly: `nextMsg(0)` is non-blocking, a message for subscription B survives a blocking wait on A, coalesced frames, `unsubscribe` discarding, fail-loud pending limit, binary payloads, and HMSG from a raw `HPUB` peer. |
| `tests/t_parser_fuzz.nim` | – | chunk-boundary invariance under randomized frame sequences and splits, randomized payload bytes, exhaustive 2-way splits, and the pinned `-ERR`/`INFO` argument quirks. |
| `tests/t_shim.nim` | – | the compatibility contract: status-vs-error semantics, `data`+`dataLen` binary safety, cleanup after a timeout. |
| `tests/busharness.nim`, `tests/responder.nim` | – | fixtures: server lifecycle (config file, readiness probe) and an echo responder process, needed because `request()` blocks and a single-threaded test cannot service the other end. |

## Translation rules

1. **Provenance per file.** A ported file starts with
   `## Derived from nats-io/nats.go <path> @ 1ffb90b (Apache-2.0).`
2. **Tests travel with the code.** A ported module is not done until the
   corresponding upstream `*_test.go` cases exist as Nim tests and pass.
   Tests ported from upstream carry the same header.
3. **Behavior, not architecture.** Go's goroutines/channels/mutexes are
   *not* translated. The client is single-threaded and poll-driven; the
   state machine is ported, the driver is rewritten.
4. **No silent feature drop.** Anything deliberately unsupported (TLS,
   nkeys/JWT, JetStream, compression) is documented in the README and
   returns a clear error rather than behaving subtly differently.
5. **Upstream is the spec.** Where Nim and Go idioms disagree, follow Go's
   observable behavior and note the deviation in the commit message.
