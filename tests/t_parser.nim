## Parser + contract tests. P0 placeholder: only the shim contract exists.
##
## P1 replaces this with the cases translated from nats.go's parser tests
## (see PROVENANCE.md).
import std/unittest
import nats

suite "shim contract":
  test "checkStatus accepts NATS_OK only":
    check checkStatus(NATS_OK)
    check not checkStatus(NATS_TIMEOUT)

  test "the surface used by Niffler resolves":
    # compile-time contract: these must exist with these signatures
    var nc = NatsConnection(conn: nil)
    check nc.conn == nil
    let p: proc (url: string): NatsConnection = connect
    check not p.isNil
