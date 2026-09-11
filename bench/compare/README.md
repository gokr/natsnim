# bench/compare — head-to-head against nats.go

Measures this client against `nats-io/nats.go` on the same private
`nats-server`: request/reply latency at three payload sizes, publish fan-out
throughput across publisher/subscriber pairings, and connect/handshake cost.

```bash
python3 bench/compare/run.py
```

- Builds both peers on first use (`nim c -d:release` for the Nim peer; the
  Go peer needs the Go toolchain and fetches `nats.go v1.41.1`).
- Needs a `nats-server`: set `NATS_SERVER_BIN` or have `nats-server` on
  `PATH`. `NATS_BENCH_PORT` picks the private server's port (default 43333).
- Go's `Publish` is batched by its flusher goroutine; the Nim equivalent is
  the explicit batch API, so the throughput matrix includes `nim(pubbatch)`.
  `nim(pub)` is this client's default write-through publish, kept to show
  what the batch API buys.

Results are hardware- and load-sensitive: compare ratios **within one run**
(both clients always run in the same session), never absolute numbers across
runs or machines. Numbers quoted in the top-level README came from this
harness on one machine; regenerate for your own hardware.
