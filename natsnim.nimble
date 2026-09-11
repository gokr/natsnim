# Package

version       = "0.1.0"
author        = "Göran Krampe"
description   = "Pure-Nim NATS client (core NATS) — a translation of nats-io/nats.go"
license       = "Apache-2.0"
srcDir        = "src"
skipDirs      = @["tests"]

requires "nim >= 2.2.0"

task test, "Run the test suite":
  exec "nim c --hints:off --path:src -o:tests/bin/responder tests/responder.nim"
  exec "nim c -r --hints:off --path:src -o:tests/bin/t_parser tests/t_parser.nim"
  exec "nim c -r --hints:off --path:src -o:tests/bin/t_parser_fuzz tests/t_parser_fuzz.nim"
  exec "nim c -r --hints:off --path:src -o:tests/bin/t_nuid tests/t_nuid.nim"
  exec "nim c -r --hints:off --path:src -o:tests/bin/t_subject tests/t_subject.nim"
  exec "nim c -r --hints:off --path:src -o:tests/bin/t_bus tests/t_bus.nim"
  exec "nim c -r --hints:off --path:src -o:tests/bin/t_shim tests/t_shim.nim"
  exec "nim c -r --hints:off --path:src -o:tests/bin/t_reconnect_opts tests/t_reconnect_opts.nim"
  exec "nim c -r --hints:off --path:src -o:tests/bin/t_reconnect tests/t_reconnect.nim"
  exec "nim c -r --hints:off --path:src -o:tests/bin/t_hardening tests/t_hardening.nim"
  exec "nim c -r --hints:off --threads:on --path:src -o:tests/bin/t_threads tests/t_threads.nim"
  exec "nim c --hints:off --path:src -o:tests/bin/faultclient tests/faultclient.nim"
  exec "python3 tests/t_faultpeer.py"

task testRequired, "Run all tests; fail if nats-server is missing":
  putEnv("NATS_REQUIRE_SERVER", "1")
  exec "nimble test"

task bench, "Local request latency microbenchmark":
  exec "nim c -r -d:release --hints:off --path:src -o:tests/bin/bench_requests tests/bench_requests.nim"
