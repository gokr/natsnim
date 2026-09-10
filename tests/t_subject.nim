## Subject and queue-name validation, mirroring nats-io/nats.go's `badSubject`
## and `badQueue` (@ 1ffb90b). Upstream has no dedicated test for these, so the
## cases below are derived from the implementation's stated semantics.

import std/unittest
import natsnim/subject

suite "subject validation":

  test "valid subjects":
    for s in ["a", "foo", "foo.bar", "a.b.c", "_INBOX.abc123.42",
              "$JS.ACK.stream.consumer", "x-y_z", "a.b.c.d.e.f"]:
      check validSubject(s)

  test "invalid subjects":
    for s in ["", ".", "a.", ".a", "a..b", "..", "a b", "a\tb", "a\rb",
              "a\nb", " ", "a. b"]:
      check badSubject(s)

  test "queue names":
    check not badQueue("workers")
    check not badQueue("pool-1.0")
    for q in ["two words", "a\tb", "a\rb", "a\nb"]:
      check badQueue(q)
