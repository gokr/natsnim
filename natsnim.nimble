# Package

version       = "0.1.0"
author        = "Göran Krampe"
description   = "Pure-Nim NATS client (core NATS) — a translation of nats-io/nats.go"
license       = "Apache-2.0"
srcDir        = "src"
skipDirs      = @["tests"]

requires "nim >= 2.2.0"

task test, "Run the test suite":
  exec "nim c -r --hints:off --path:src -o:tests/bin/t_parser tests/t_parser.nim"
  exec "nim c -r --hints:off --path:src -o:tests/bin/t_nuid tests/t_nuid.nim"
  exec "nim c -r --hints:off --path:src -o:tests/bin/t_subject tests/t_subject.nim"
