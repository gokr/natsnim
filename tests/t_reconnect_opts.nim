## Unit tests for the reconnect knobs — pure logic, no server, no network.
##
## The point of these two helpers is to be testable: the *behaviour* they shape
## (blocking on a black-hole dial; a lockstep retry storm) cannot be produced
## reliably in a test, but the arithmetic that decides it can.

import std/[random, unittest]
import nats/conn as core
import nats

proc opts(waitMs, jitterMs, dialMs, connectMs: int): core.DialOptions =
  result = core.defaultDialOptions()
  result.reconnectWaitMs = waitMs
  result.reconnectJitterMs = jitterMs
  result.reconnectDialTimeoutMs = dialMs
  result.connectTimeoutMs = connectMs

suite "reconnect options":

  test "the reconnect dial gets its own budget":
    check core.dialTimeoutForAttempt(opts(50, 0, 1500, 9000)) == 1500
    # 0 means "reuse the initial connect timeout"
    check core.dialTimeoutForAttempt(opts(50, 0, 0, 9000)) == 9000

  test "jitter is added to the wait, within bounds":
    var r = initRand(1)
    check core.reconnectDelayMs(opts(200, 0, 0, 5000), r) == 200  # no jitter
    let o = opts(200, 100, 0, 5000)
    for _ in 0 ..< 200:
      let d = core.reconnectDelayMs(o, r)
      check d >= 200
      check d <= 299

  test "jitter actually decorrelates two connections":
    # The property that matters: two clients that lost the bus at the same
    # moment do not compute the same delay every time.
    var ra = initRand(11)
    var rb = initRand(22)
    let o = opts(200, 100, 0, 5000)
    var differing = 0
    for _ in 0 ..< 50:
      if core.reconnectDelayMs(o, ra) != core.reconnectDelayMs(o, rb):
        inc differing
    check differing > 30

suite "shim status text":
  test "getErrorString distinguishes ok/timeout/error":
    check getErrorString(NATS_OK) == "ok"
    check getErrorString(NATS_TIMEOUT) == "timeout"
    # NATS_ERR carries the last failure's message; in a process that has not
    # failed yet it is empty (t_shim asserts the populated case after a real
    # failure). Either way it must not masquerade as ok/timeout.
    check getErrorString(NATS_ERR) != "ok"
    check getErrorString(NATS_ERR) != "timeout"
