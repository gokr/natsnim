## Core-NATS transport tests against a real `nats-server`.
##
## Upstream (nats.go) tests this through its reader goroutine and per-sub
## channels. This port is poll-driven and single-threaded, so the tests below
## target the properties that *that* design can get wrong — several of which
## have no upstream counterpart:
##
##   * `nextMsg(0)` must not wait (Niffler's requestEnvelope depends on it),
##   * a message for subscription B that arrives while blocking on A must not
##     be lost (there is no background reader to have already queued it),
##   * coalesced frames in one read are all delivered,
##   * `unsubscribe` discards locally queued messages,
##   * the pending limit fails loud instead of growing without bound,
##   * binary payloads (NUL bytes) survive byte-for-byte,
##   * HMSG from a raw `HPUB` peer is split into headers + body.

import std/[net, sets, strutils, times, unittest]
import natsnim/conn as core
import busharness


proc rawPeer(port: int): Socket =
  ## A hand-rolled client, so a test can send frames this library never emits
  ## (HPUB, in particular).
  result = newSocket()
  result.connect("127.0.0.1", Port(port))
  let info = result.recvLine()
  doAssert info.startsWith("INFO "), "unexpected greeting: " & info
  result.send("CONNECT {\"verbose\":false,\"headers\":true}\r\n")
  result.send("PING\r\n")
  doAssert result.recvLine().startsWith("PONG"), "no PONG from the server"

proc runTests(url: string, port: int) =

  suite "core NATS transport":

    test "handshake completes and server INFO is parsed":
      let c = core.dial(url)
      defer: c.close()
      check c.info.serverId.len > 0
      check c.info.version.len > 0
      check c.maxPayload == 1024          # from the generated config file
      check c.info.maxPayload == 1024

    test "publish/subscribe round trip on one connection (echo)":
      let c = core.dial(url)
      defer: c.close()
      let sub = c.subscribe("t.echo")
      c.flush()
      c.publish("t.echo", "hello")
      let m = sub.nextMsg(2000)
      check m.subject == "t.echo"
      check m.data == "hello"
      check m.reply == ""
      check m.headers == ""
      check m.size == 5

    test "nextMsg(0) does not wait for a message":
      let c = core.dial(url)
      defer: c.close()
      let sub = c.subscribe("t.idle")
      c.flush()
      let t0 = epochTime()
      expect core.NatsTimeout:
        discard sub.nextMsg(0)
      check epochTime() - t0 < 0.25

    test "a message for another subscription is not lost while waiting":
      # The invariant of a poll-driven client: blocking on A must still
      # demultiplex B's message into B's queue.
      let c = core.dial(url)
      defer: c.close()
      let a = c.subscribe("t.a")
      let b = c.subscribe("t.b")
      c.flush()
      c.publish("t.b", "for-b")
      c.flush()
      expect core.NatsTimeout:
        discard a.nextMsg(300)          # waits, sees nothing for A
      let m = b.nextMsg(0)              # ...and B's message is already queued
      check m.data == "for-b"

    test "a 1 ms timeout still reads an available message":
      # Regression: with a 1 ms budget, `inMilliseconds` truncates the
      # remaining time to 0, so an "out of budget" early return made the whole
      # call a no-op — the socket was never read. Components pump at 25 ms
      # (unaffected); a 1 ms poll is what Niffler's core registry uses, and it
      # silently never saw registrations.
      let c = core.dial(url)
      let pub = core.dial(url)
      defer:
        c.close()
        pub.close()
      let sub = c.subscribe("t.one")
      c.flush()
      pub.publish("t.one", "x")
      pub.flush()                    # the message is in our socket now
      check sub.nextMsg(1).data == "x"        # must read, not time out
      pub.publish("t.one", "y")
      pub.flush()
      check sub.nextMsg(1).data == "y"
      # and an empty 1 ms poll still returns promptly with a timeout
      let t0 = epochTime()
      expect core.NatsTimeout:
        discard sub.nextMsg(1)
      check epochTime() - t0 < 0.5

    test "coalesced frames from one read are all delivered":
      let c = core.dial(url)
      defer: c.close()
      let sub = c.subscribe("t.many")
      c.flush()
      for i in 0 ..< 5:
        c.publish("t.many", "m" & $i)
      c.flush()
      var got: seq[string]
      for _ in 0 ..< 5:
        got.add sub.nextMsg(1000).data
      check got == @["m0", "m1", "m2", "m3", "m4"]

    test "request/reply end to end (responder process)":
      let resp = startResponder(url, "svc.echo", "re:")
      defer: stopResponder(resp)
      let c = core.dial(url)
      defer: c.close()
      let m = c.request("svc.echo", "ping", 4000)
      check m.subject.len > 0
      check m.data == "re:ping"

    test "a request with no responders fails fast instead of timing out":
      # The server answers a request whose subject has no subscribers with a
      # 503 status message, but only if the client advertised
      # `no_responders: true`. Without that (and without parsing the status)
      # every probe of an absent component costs the full request timeout —
      # which in Niffler pushed real work past a runner's idle window.
      let c = core.dial(url)
      defer: c.close()
      let t0 = epochTime()
      var raised = false
      try:
        discard c.request("nobody.listening", "x", 5000)
      except core.NoRespondersError:
        raised = true
      check raised
      check epochTime() - t0 < 1.0        # not the 5 s timeout
      # the connection is still usable afterwards
      let sub = c.subscribe("t.after")
      c.flush()
      c.publish("t.after", "still-here")
      check sub.nextMsg(2000).data == "still-here"

    test "request() times out and leaves no inbox subscription behind":
      # Needs a subject that *has* a subscriber which never answers: with no
      # subscriber at all the fail-fast path (503) applies, not a timeout.
      let c = core.dial(url)
      defer: c.close()
      let silent = c.subscribe("t.silent")     # never replies
      c.flush()
      let before = c.subscriptionCount
      expect core.NatsTimeout:
        discard c.request("t.silent", "x", 250)
      check c.subscriptionCount == before      # the inbox was removed
      silent.unsubscribe()

    test "queue group delivers each message exactly once":
      let pub = core.dial(url)
      let c = core.dial(url)
      defer:
        pub.close()
        c.close()
      var subs: seq[core.Subscription]
      for _ in 0 ..< 3:
        subs.add c.subscribe("q.work", "workers")
      c.flush()
      for i in 0 ..< 9:
        pub.publish("q.work", "w" & $i)
      pub.flush()
      var got: seq[string]
      for _ in 0 ..< 60:
        for s in subs:
          let m = s.tryNextMsg(20)
          if m != nil: got.add m.data
        if got.len == 9: break
      check got.len == 9
      var uniq = initHashSet[string]()
      for g in got: uniq.incl g
      check uniq.len == 9        # each message delivered exactly once

    test "a wildcard subscription receives matching subjects":
      # Core's component registry is `reg.>`; Niffler's console uses `>`.
      # The server does the matching, so this is really a test that our SUB
      # frame is well formed and that routing by sid works for wildcards.
      let c = core.dial(url)
      defer: c.close()
      let reg = c.subscribe("reg.>")
      let all = c.subscribe(">")
      c.flush()
      c.publish("reg.publish", "reg-event")
      c.publish("other.topic", "other-event")
      c.flush()
      let m = reg.nextMsg(1000)
      check m.subject == "reg.publish"
      check m.data == "reg-event"
      # `>` sees both, but not itself-once-more: exactly two messages
      var seen: seq[string]
      for _ in 0 ..< 2:
        seen.add all.nextMsg(1000).subject
      check seen == @["reg.publish", "other.topic"]
      expect core.NatsTimeout:
        discard reg.nextMsg(50)

    test "two subscribers on the same subject both receive (fan-out)":
      let c = core.dial(url)
      defer: c.close()
      let s1 = c.subscribe("t.fan")
      let s2 = c.subscribe("t.fan")
      c.flush()
      c.publish("t.fan", "both")
      c.flush()
      check s1.nextMsg(2000).data == "both"
      check s2.nextMsg(2000).data == "both"

    test "unsubscribe discards locally queued messages and stops delivery":
      let c = core.dial(url)
      defer: c.close()
      let sub = c.subscribe("t.unsub")
      c.flush()
      c.publish("t.unsub", "gone")
      c.flush()
      check sub.pendingCount == 1
      sub.unsubscribe()
      check sub.pendingCount == 0
      check c.subscriptionCount == 0
      c.publish("t.unsub", "later")
      c.flush()
      check sub.pendingCount == 0

    test "pending limit fails loud instead of growing without bound":
      let c = core.dial(url)
      defer: c.close()
      let sub = c.subscribe("t.flood", pendingLimit = 2)
      c.flush()
      for i in 0 ..< 6:
        c.publish("t.flood", "x" & $i)
      c.flush()
      check sub.pendingCount == 2
      check sub.overrun
      check sub.dropped == 4
      expect core.NatsError:
        discard sub.nextMsg(0)

    test "binary payloads survive byte-for-byte":
      let c = core.dial(url)
      defer: c.close()
      let sub = c.subscribe("t.bin")
      c.flush()
      let payload = "\x00\x01\xffMSG \r\nPUB t 9\r\n\x00\x7f"
      c.publish("t.bin", payload)
      let m = sub.nextMsg(2000)
      check m.data.len == payload.len
      check m.data == payload
      check m.size == payload.len

    test "a payload at exactly max_payload is accepted, one byte more is not":
      let c = core.dial(url)
      defer: c.close()
      let sub = c.subscribe("t.cap")
      c.flush()
      let full = repeat("z", 1024)
      c.publish("t.cap", full)
      check sub.nextMsg(2000).data.len == 1024
      var raised = false
      try:
        c.publish("t.cap", full & "z")
      except core.NatsError as e:
        raised = true
        check e.msg.contains("max_payload")
      check raised

    test "invalid subjects and queue names are rejected before the wire":
      let c = core.dial(url)
      defer: c.close()
      for bad in ["", ".", "a..b", "a b"]:
        var raised = false
        try: discard c.subscribe(bad)
        except core.NatsError: raised = true
        check raised
      var raised = false
      try: c.publish("a b", "x")
      except core.NatsError: raised = true
      check raised
      raised = false
      try: discard c.subscribe("ok.subject", "bad queue")
      except core.NatsError: raised = true
      check raised

    test "HMSG from a raw HPUB peer arrives as headers + body":
      let c = core.dial(url)
      defer: c.close()
      let sub = c.subscribe("t.hdr")
      c.flush()
      let raw = rawPeer(port)
      defer: raw.close()
      let hdrs = "NATS/1.0\r\nX-Test: 1\r\n\r\n"
      let body = "body-bytes"
      raw.send("HPUB t.hdr " & $hdrs.len & " " & $(hdrs.len + body.len) &
               "\r\n" & hdrs & body & "\r\n")
      raw.send("PING\r\n")
      discard raw.recvLine()
      let m = sub.nextMsg(2000)
      check m.headers == hdrs
      check m.data == body
      check m.size == hdrs.len + body.len

    # Server-PING behavior is tested by t_faultpeer.py's explicit PING/PONG
    # exchange, not by sleeping less than the real server's ping interval.

proc main() =
  if not serverAvailable():
    skipBanner()
    quit(0)
  stderr.writeLine("[t_bus] server available, starting")
  let srv = startServer(maxPayload = 1024)
  defer: srv.stop()
  stderr.writeLine("[t_bus] running tests against " & srv.url)
  runTests(srv.url, srv.port)
  stderr.writeLine("[t_bus] done")

main()
