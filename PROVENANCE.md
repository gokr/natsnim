# Provenance

This library is a Nim translation of the official Go client:

- **Source:** https://github.com/nats-io/nats.go
- **Pinned commit:** `1ffb90b` (2026-09-10) — record the exact SHA before the
  first ported file lands, and pin the whole port to it.
- **License:** Apache-2.0 (see [LICENSE](LICENSE), [NOTICE](NOTICE))

Per-file map. Each ported Nim file must carry a header naming its upstream
origin, and this table is the authoritative list. Empty until P1.

| Nim | upstream | notes |
|---|---|---|
| _none yet_ | | |

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
