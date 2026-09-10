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
