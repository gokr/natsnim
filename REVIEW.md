# natsnim code review — 2026-09-11

Reviewed revision: `9d1932e`. Scope: every production Nim module, every Nim
fixture/test, and `natsnim.nimble` (3,348 lines of `.nim` source). This is a
review of **natsnim**, not a full review of the separate Niffler repository.

> **Fix status: all items below are fixed on `main` (commit range
> `9d1932e..HEAD`), each with a regression test.** Sections are kept as
> written for the record; the *Fix* paragraphs describe what shipped. Where a
> fix narrowed a claim, the README now states the narrowed contract.

**Recommendation: keep the Niffler integration opt-in. Fix the high-priority
items below before switching its default client.** Happy-path protocol tests
are useful, but do not establish lifecycle, deadline, or failure-path safety.

No library implementation changes were made during this review. Probes and a
TCP_NODELAY-only experimental copy are under `/tmp/natsnim-review/`. Results
below were obtained with Nim 2.2.10 on Linux. Performance numbers are local
microbenchmarks, not Niffler end-to-end measurements.

## High priority

### 1. Every shim message leaks, and connection/subscription handles also leak

**Locations:** `src/natsnim.nim:98–113,183–200,228–237,280–294,311–314`.

`create(T)` is raw allocation, not a GC-managed object allocation. Nim's own
`system/memalloc.nim` explicitly requires freeing it. `natsMsg_Destroy` is a
no-op, so both the handle and its managed string fields remain retained.
Subscription/connection destruction clears some fields but never deallocates
the handles. Convenience `close` does not even clear `handle.impl` before
losing the pointer. A failed `connect` also leaks its preallocated handle.

**Reproduced:** 2,000 received 1 KiB messages, each passed to `natsMsg_Destroy`,
followed by subscription destruction, connection close and `GC_fullCollect`,
retained **2,277,392 additional bytes**. This is sustained growth on the normal
Niffler message path, not just an exceptional-path leak.

**Fix:** establish explicit ownership for all three pointer handles. Destroy
managed fields before deallocating raw storage; clean up failed construction.
Audit aliasing/copy semantics of `NatsConnection` before adding frees. Test
repeated full lifecycles under ORC and any other supported memory manager.
Correct the misleading GC documentation in the module header and destroy procs.

### 2. Writes are unbounded; the installed std/net send loop also has hazards

**Locations:** `src/natsnim/conn.nim:243–260,388–392,640–656`.

The socket is restored to blocking mode and all writes call `sock.send(data)`
without a deadline. A non-reading peer can stall publish, request, flush,
handshake, resubscription, and even the PONG callback indefinitely.

**Reproduced:** a fake peer completes the handshake, stops reading, then closes
at 700 ms. An 8 MiB `request(..., 20)` still had not returned after **5 seconds**;
the probe was killed by its external timeout.

There is an additional version-specific dependency hazard: in the installed
Nim 2.2.10 `net.nim:1739–1771`, the string-send loop retries using the original
pointer/length rather than advancing by `written`. Its default `SafeDisconn`
can suppress a disconnect exception without making progress or consuming the
retry limit. Do not assume this helper provides correct partial-write/error
handling merely because it compiles.

**Fix:** own a bounded write loop using the pointer/length send primitive,
advance the offset on partial sends, use nonblocking I/O + POLLOUT and a
monotonic deadline, handle EINTR/EAGAIN, and terminate on disconnect/zero
progress. Keep this synchronous: no asyncdispatch or background thread needed.
Add slow-reader, partial-write, reset-during-write and interrupted-write tests.

### 3. Failed initial handshakes leak sockets

**Location:** `src/natsnim/conn.nim:568–584`.

`dial` assigns the socket and calls `handshake` with no failure cleanup. If
handshake times out or rejects a greeting/authentication, the caller never
receives a connection it can close. The reconnect path has cleanup; initial
dial does not.

**Reproduced:** ten connections to a peer that accepts TCP but never sends INFO
left **ten additional `/proc/self/fd` entries**, even after `GC_fullCollect`.

**Fix:** close the socket and release partial connection state on every
post-connect construction failure. Test repeated auth, INFO and PONG failures,
not just a dead port (which fails before this leak).

### 4. Caller deadlines do not constrain reconnection

**Locations:** `src/natsnim/conn.nim:475–526,546–566,679–743,755–795`.

`ensureConnected` invokes a full dial and handshake without receiving the
caller's remaining budget. `awaitConnected` checks its deadline only after
that attempt. `request` additionally starts its response deadline *after*
waiting for connectivity, effectively granting another complete timeout.
Even `nextMsg(0)` can attempt a blocking reconnect.

**Reproduced:** with a 400 ms handshake budget and a reconnect peer withholding
INFO, **`nextMsg(5)` took 401 ms**. Defaults permit much longer overruns.

**Fix:** create one absolute deadline at each public operation's entry and pass
it through dial, handshake, write, reconnect waiting and response waiting.
For truly nonblocking polls, either advance a connection-local reconnect state
machine or explicitly avoid attempts requiring a blocking operation. Account
for DNS resolution too; a timed TCP connect does not itself bound DNS.

### 5. Untrusted numeric protocol fields can crash the process

**Locations:** `src/natsnim/parser.nim:96–104,211–224`.

Decimal accumulation has no overflow check. Even a successfully parsed size
near `high(int64)` overflows the parser's `argStart + size - 1` arithmetic.
These become `OverflowDefect`, not the documented parse error or a catchable
`NatsError`. Disabling checks is not a fix; wrapped sizes break framing.

**Reproduced in normal and `-d:release` builds:**

```text
MSG x 9223372036854775808 0\r\n\r\n
MSG x 1 9223372036854775807\r\n
```

Both raise `OverflowDefect`. This requires a malicious/malformed server or
wire peer, not an ordinary publisher behind a correctly validating server.

**Fix:** checked decimal conversion, checked conversion to platform `int`, and
bounded/incremental payload arithmetic. Extend fuzzing to malformed numbers,
integer boundaries and arbitrary bytes; current generators mostly produce
valid small frames.

### 6. Memory limits are insufficient against large inbound traffic

**Locations:** `src/natsnim/parser.nim:159–410`;
`src/natsnim/conn.nim:34,194–222,314`.

Control-line and partial-payload buffers have no enforced size cap. The exported
`MAX_CONTROL_LINE_SIZE` is not enforced. Server `max_payload` is checked only
when publishing. Subscription limits count messages, not bytes: 65,536 queued
1 MiB messages allow roughly **64 GiB per subscription**, before overhead.
The `serverErrors` sequence is also unbounded.

**Fix:** configurable inbound control/payload limits, subscription byte limits,
an aggregate connection budget, and bounded error history. Reject oversized
frames before buffering them. Check limits before allocating/splitting messages
that will be dropped. A malicious server can exploit framing limits; ordinary
high-volume publishers can trigger the pending-byte problem.

## Correctness and operational behavior

### 7. Server errors are recorded but not surfaced after handshake

**Locations:** `src/natsnim/conn.nim:310–320,755–771`.

`onErr` appends strings; normal receive/flush paths do not act on them. A denied
publish can be followed by a successful flush. Denied subscriptions can appear
installed and subsequently time out rather than reporting permissions errors.
In a client with no async error callback, these failures are effectively silent
unless the application knows to inspect a public implementation field.

**Reproduced:** the peer sent a publish-permission `-ERR`, then answered PING.
`flush` returned success while `serverErrors` contained the permission error.
A flush is not a publish acknowledgement, so the fix need not pretend otherwise.

**Fix:** define an explicit bounded error polling/event API, and classify
terminal connection errors versus permission errors. Ensure the SDK pumps this
channel. Do not silently keep using a parser after a protocol error either:
`feed` currently raises without marking the connection unusable.

### 8. Header status handling drops valid application data and delays errors

**Locations:** `src/natsnim/conn.nim:194–216,698–743`.

Every HMSG with status >= 300 is removed from the message stream and put into a
single status slot, regardless of payload. This loses application messages,
collapses multiple statuses, and changes ordering. Upstream recognizes the
no-responders sentinel specifically as an **empty-body 503**; it does not
convert all application statuses into subscription errors.

**Reproduced:** an HMSG with `NATS/1.0 503 app-status` and body `body` raised
`NoRespondersError`; the body was lost.

There is a separate polling bug: status is checked before `pump`, but not after
it before the zero-timeout/deadline exit. A newly read 503 can be reported as a
timeout, then as no-responders on the next call. **Reproduced:** two consecutive
`nextMsg(0)` calls returned `NatsTimeout`, then `NoRespondersError`.

**Fix:** queue messages/status in wire order, recognize only the appropriate
empty-body sentinel, and apply the same result checks immediately after reads.

### 9. Publishing succeeds when reconnection is permanently disabled/exhausted

**Location:** `src/natsnim/conn.nim:640–668`.

The disconnected publish path checks `opts.reconnect`, but not
`maxReconnects == 0` or exhausted attempts. It returns success and buffers data
that no future attempt can send. This contradicts the README's fail-fast claim.

**Reproduced:** a connection with `maxReconnects = 0`, placed in its documented
disconnected state, accepted a publish. Source inspection shows the same path
after attempt exhaustion. The existing zero-reconnect test tests receiving,
not publishing.

**Fix:** centralize reconnect-eligibility checks and reject new publishes once
there is no future reconnect. Specify what happens to already buffered data.

### 10. `unsubscribe(maxMsgs)` immediately detaches instead of auto-unsubscribing

**Location:** `src/natsnim/conn.nim:622–638`.

The optional count is sent to the server, but the subscription is immediately
closed and removed locally and its queue discarded. A caller requesting an
auto-unsubscribe after N messages receives none of the future messages.

**Fix:** implement delivered-count/max semantics (including reconnect replay),
or remove the unsupported parameter until implemented. Test it separately from
immediate unsubscribe.

### 11. One connection per thread is not sufficient for thread safety

**Locations:** `src/natsnim/nuid.nim:88–98`; `src/natsnim.nim:56–78`.

All connections use the same mutable `globalNuID` / `globalReady` through
`newInbox`; shim errors also share a global string. Separate connection owners
can still access these globals concurrently. The README recommends one
connection per thread, but the implementation does not satisfy that contract.

**Fix:** per-connection NUID and error state, or appropriately owned thread-local
state. No need to introduce internal worker threads. If the intended contract
is one OS thread for the entire library, state and enforce that instead.

### 12. Reconnect jitter is redrawn on every eligibility check

**Location:** `src/natsnim/conn.nim:521–525`.

The next retry threshold changes on each poll. Frequent polling tends to find
a small draw shortly after the minimum delay, narrowing the intended jitter
and making retry timing depend on poll frequency. Current tests verify only
the standalone random helper, not scheduling.

**Fix:** draw once per attempt and store `nextAttemptAt`. Reset scheduling state
at well-defined disconnect/success transitions. Test fake-clock scheduling.

## Performance

### 13. Missing TCP_NODELAY adds a large avoidable localhost roundtrip penalty

**Locations:** `src/natsnim/conn.nim:375–395,777–795`.

The connection makes consecutive small writes (SUB, PUB, later UNSUB) without
turning off Nagle. Combined with delayed ACK this produces approximately 40 ms
of overhead in a basic request path on this machine.

**Measured:** 20 sequential no-responder requests against an isolated local
NATS server took **829.1 ms**. Rebuilding an otherwise identical temporary copy
with only `OptNoDelay = true` at `IPPROTO_TCP` reduced this to **3.49 ms**.
This is a diagnostic microbenchmark, not a promise of that speedup for every
workload. It is nevertheless a clear first optimization.

**Fix:** enable TCP_NODELAY for both initial and reconnect sockets; consider
batching related protocol writes. Add request latency/throughput benchmarks.

### Further optimizations, after correctness fixes

Status after the hardening pass — all four are resolved or consciously
retired:

- ~~`readSome` allocates a new receive string every call; `readBuf` is
  unused~~ — fixed: one reused receive buffer feeds the parser.
- ~~`publishRaw` builds a full extra frame copy~~ — fixed: publishes assemble
  into a reused output buffer (no per-publish concatenation), payloads ≥ 16
  KiB on an empty buffer bypass it entirely, and multi-frame operations
  coalesce into one write. Cross-call batching like nats.go's was attempted
  and rejected: without a flusher thread it deadlocks "publish on A, read on
  B" patterns (pinned by a regression test); publish now writes through.
- ~~`deliver` constructs/splits a message before checking pending
  capacity~~ — fixed: limits are checked before any message allocation.
- ~~`waitReadable` treats EINTR as a dead connection~~ — fixed: EINTR retries
  within the remaining deadline.

Still open, deliberately: a shared wildcard request inbox (Go's mux) to
remove the per-request SUB/UNSUB pair, and parser zero-copy payload delivery
(owned strings are a safety choice; revisit only with benchmarks in hand).

## Cleanup and test quality

- Correct stale documentation: `conn.nim` still says reconnect is unsupported;
  README/module comments still mention `nats/conn`, 65 tests, no callbacks
  (there are internal parser callbacks), and several unproven compatibility
  guarantees. The C API's `natsSubscription_Destroy` is actually **void**, so
  the claimed return-type deviation is incorrect.
- `nuid.nim:60–68`: Nim `rand(max)` is inclusive. It can generate increment 333
  and sequence `maxSeq`, contrary to upstream's half-open ranges. Use `max-1`
  bounds and boundary tests. Low practical collision risk, but an incorrect
  ported contract.
- Validate option ranges and supported URLs explicitly; do not echo a complete
  credential-bearing URL in errors (`parseUrl:346–371`). Percent-decoded
  credentials/IPv6/token support should be either implemented or documented.
- Hide state that callers should not mutate (`connected`, `closed`, sid and
  connection links), and expose read-only accessors plus deliberate test seams.
- Remove unused imports/fields/constants (`readBuf`, `argsLenMax`,
  `hdrArgsLenMax`, etc.) and unused fixture arguments.
- `t_reconnect.noticeOutage` catches `NatsError` before its `NatsTimeout`
  subtype: an ordinary timeout is accepted as proof of disconnection. Assert
  actual disconnected state and place subtype catches first.
- The keepalive test sleeps 1.2 seconds against a default two-minute ping
  interval. It does **not** demonstrate server-PING handling. Configure a short
  interval or use a deterministic fake server.
- Bus tests silently succeed when no server is installed (with a printed skip).
  Add a CI/required-integration mode that fails if prerequisites are absent.
- `busharness.startServer` uses one predictable directory per PID and
  bind-close-rebind port selection; multiple live servers in a process collide
  on files, and another process can win the port. Use unique temp directories,
  server-assigned ports, verify the child remains alive, and clean up on failed
  startup. Avoid probe connections using the very handshake implementation
  under test as the only readiness oracle.
- Some shim tests omit message destruction. Make test cleanup obey the intended
  ownership contract so that leak tests remain meaningful after fixing it.

## Suggested implementation order

1. Ownership + failed-construction cleanup, with allocation/FD regression tests.
2. Deadline-aware read/write/reconnect state handling; test stalled and reset
   peers and the actual supported Nim toolchain.
3. Numeric/buffer bounds and deterministic error/status delivery.
4. Reconnect terminal-state, unsubscribe and global-state correctness.
5. TCP_NODELAY (small, independently benchmarked change), then profile copying.
6. Malformed-wire tests, fake-clock/fake-peer tests, required integration mode,
   and differential validation against the reference clients.

The existing parser chunk-boundary/property tests and cross-subscription
routing tests are worth retaining. Extend them with failure/lifecycle tests
rather than replacing them or interpreting a green happy-path suite as proof
that the above failure modes cannot occur.
