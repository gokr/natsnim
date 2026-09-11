## Reconnect tests (P5).
##
## This is where a no-threads client differs most from nats.go: there is no
## background reconnector, so reconnection is **lazy** — driven by the calls
## that wait (`nextMsg`/`flush`/`request`), gated to at most one attempt per
## `reconnectWaitMs` so a 1 ms poll neither spins nor blocks on every call.
## Upstream cannot test any of this against a server restart from the same
## process; here the harness restarts the server on its original port.
##
## The properties asserted:
##   * a server restart is noticed and reconnected transparently,
##   * subscriptions are re-registered **with their original sids**, so
##     messages keep flowing and a reply subject issued before the outage
##     still routes after it,
##   * publishes issued while disconnected are buffered and delivered on
##     reconnect, in order,
##   * past the buffer cap a publish fails instead of being silently dropped,
##   * `reconnect: false` / `maxReconnects: 0` make the outage fatal, and
##     then publishes fail fast rather than buffering forever,
##   * an explicit `close` is not an outage: nothing reconnects.

import std/[strutils, times, unittest]
import natsnim/conn as core
import busharness

proc fastOpts(maxReconnects = 30, waitMs = 50, bufSize = 8 * 1024 * 1024,
              reconnect = true): core.DialOptions =
  result = core.defaultDialOptions()
  result.reconnectWaitMs = waitMs
  result.maxReconnects = maxReconnects
  result.reconnectBufSize = bufSize
  result.reconnect = reconnect

proc noticeOutage(c: core.Connection, sub: core.Subscription) =
  ## Drive the client until it has noticed the dead socket. Deliberately not
  ## asserting *which* call notices: the write may fail (RST already in) or the
  ## read may (FIN), and both are correct.
  for _ in 0 ..< 40:
    try:
      discard sub.nextMsg(100)
    except core.NatsTimeout:
      if not c.connected: return
    except core.NatsError:
      if not c.connected: return
      raise
  doAssert false, "client never noticed the outage"

proc runTests() =

  suite "reconnect":

    test "a server restart is reconnected transparently, sids preserved":
      var srv = startServer(maxPayload = 1024)
      defer: srv.stop()
      let c = core.dial(srv.url, fastOpts())
      defer: c.close()
      let sub = c.subscribe("r.t")
      c.flush(2000)
      c.publish("r.t", "before")
      check sub.nextMsg(2000).data == "before"
      let sidBefore = sub.sid

      srv.restart()

      var reconnected = false
      for _ in 0 ..< 60:
        try:
          discard sub.nextMsg(100)
        except core.NatsError:
          discard
        except core.NatsTimeout:
          discard
        if c.reconnectCount > 0:
          reconnected = true
          break
      check reconnected
      check c.connected
      check sub.sid == sidBefore          # the sid is what survives
      check c.subscriptionCount == 1

      # The subscription is live again: a message published now arrives, which
      # only happens if the re-SUB carried the same sid.
      c.publish("r.t", "after")
      check sub.nextMsg(2000).data == "after"
      check sub.sid == sidBefore

      # A reply subject handed out before the outage still routes: the inbox
      # subscription was resubscribed too. Stronger than a timeout — the 503
      # for a subject nobody serves can only arrive if our re-SUB'd inbox is
      # live on the server.
      expect core.NoRespondersError:
        discard c.request("r.nobody", "x", 2000)

    test "publishes during the outage are buffered and delivered in order":
      var srv = startServer(maxPayload = 1024)
      defer: srv.stop()
      let c = core.dial(srv.url, fastOpts())
      defer: c.close()
      let sub = c.subscribe("r.buf")
      c.flush(2000)

      srv.stopServerProcess()                 # hard outage, config kept
      noticeOutage(c, sub)
      check not c.connected

      # Buffered, not dropped, and not an error.
      c.publish("r.buf", "one")
      c.publish("r.buf", "two")

      srv.startServerProcess()                # same port
      c.flush(8000)                           # waits out the reconnect window
      check c.connected
      check c.reconnectCount >= 1
      check sub.nextMsg(2000).data == "one"
      check sub.nextMsg(2000).data == "two"

    test "past the buffer cap a publish fails instead of vanishing":
      var srv = startServer(maxPayload = 1024)
      defer: srv.stop()
      let c = core.dial(srv.url, fastOpts(bufSize = 120))
      defer: c.close()
      let sub = c.subscribe("r.cap")
      c.flush(2000)

      srv.stopServerProcess()
      noticeOutage(c, sub)
      check not c.connected

      var buffered = 0
      var failed = ""
      for i in 0 ..< 20:
        try:
          c.publish("r.cap", "payload-" & $i)
          inc buffered
        except core.NatsError as e:
          failed = e.msg
          break
      check buffered > 0                       # it did buffer some
      check failed.contains("reconnect buffer exceeded")
      check buffered < 20                      # and then refused

      # A refused publish must never be half-written: after the restart the
      # delivered set is exactly the buffered prefix.
      srv.startServerProcess()
      c.flush(8000)
      check c.connected
      var got: seq[string]
      for _ in 0 ..< buffered:
        got.add sub.nextMsg(2000).data
      check got.len == buffered
      for i in 0 ..< buffered:
        check got[i] == "payload-" & $i

    test "reconnect: false makes the outage fatal and publishes fail fast":
      var srv = startServer(maxPayload = 1024)
      defer: srv.stop()
      let c = core.dial(srv.url, fastOpts(reconnect = false))
      defer: c.close()
      let sub = c.subscribe("r.off")
      c.flush(2000)
      srv.stopServerProcess()
      noticeOutage(c, sub)
      check c.reconnectCount == 0
      var raised = false
      try:
        c.publish("r.off", "x")
      except core.NatsError as e:
        raised = true
        check e.msg.contains("reconnecting is disabled")
      check raised
      var flushed = false
      try:
        c.flush(200)
      except core.NatsError as e:
        flushed = true
        check e.msg.contains("connection lost")
      check flushed

    test "maxReconnects: 0 disables reconnecting":
      var srv = startServer(maxPayload = 1024)
      defer: srv.stop()
      let c = core.dial(srv.url, fastOpts(maxReconnects = 0))
      defer: c.close()
      let sub = c.subscribe("r.zero")
      c.flush(2000)
      srv.stopServerProcess()
      noticeOutage(c, sub)
      check c.reconnectCount == 0
      var raised = false
      try:
        discard sub.nextMsg(100)
      except core.NatsError:
        raised = true
      check raised
      expect core.NatsError:
        c.publish("r.zero", "must not buffer forever")
      check c.bufferedBytes == 0

    test "an explicit close is not an outage: nothing reconnects":
      var srv = startServer(maxPayload = 1024)
      defer: srv.stop()
      let c = core.dial(srv.url, fastOpts())
      let sub = c.subscribe("r.close")
      c.flush(2000)
      c.close()
      check not c.connected
      var raised = false
      try:
        discard sub.nextMsg(100)
      except core.NatsError as e:
        raised = true
        check e.msg.contains("closed")
      check raised
      check c.reconnectCount == 0

    test "the reconnect window spaces attempts (no spin, no storm)":
      var srv = startServer(maxPayload = 1024)
      defer: srv.stop()
      # A long wait with the server down: many calls, few attempts.
      let c = core.dial(srv.url, fastOpts(maxReconnects = 100, waitMs = 400))
      defer: c.close()
      let sub = c.subscribe("r.spin")
      c.flush(2000)
      srv.stopServerProcess()
      noticeOutage(c, sub)
      let attemptsAfterNotice = c.reconnectAttempts
      let t0 = epochTime()
      for _ in 0 ..< 40:
        try:
          discard sub.nextMsg(10)
        except CatchableError:
          discard
      let elapsed = epochTime() - t0
      # 40 calls over ~400ms of wall clock: at most a couple of attempts.
      check c.reconnectAttempts - attemptsAfterNotice <= 2
      # and each nextMsg(10) still spent its budget (no hot loop)
      check elapsed >= 0.2

proc main() =
  if not serverAvailable():
    skipBanner()
    quit(0)
  runTests()

main()
