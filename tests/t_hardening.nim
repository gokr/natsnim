## Regression tests for REVIEW.md: bounds, ownership, status order, limits,
## URL/option validation, reconnect terminal states and auto-unsubscribe.
import std/[monotimes, random, strutils, times, unittest]
import natsnim
import natsnim/conn as core
import natsnim/parser as wire
import natsnim/nuid
import busharness

suite "bounded parser and input validation":
  test "overflowing numeric fields return errors rather than Defects":
    for frame in ["MSG x 9223372036854775808 0\r\n\r\n",
                  "MSG x 1 9223372036854775807\r\n",
                  "HMSG x 1 99999999999999999999999 1\r\n"]:
      var p = wire.initParser()
      check p.parse(frame, wire.Sink()).len > 0
    check wire.parseInt64("9223372036854775807") == high(int64)
    check wire.parseInt64("9223372036854775808") == -1

  test "split control and payload sizes are bounded before allocation":
    var p = wire.initParser(maxControlLine = 32, maxPayload = 128)
    check p.parse("INFO ", wire.Sink()) == ""
    check p.parse(repeat("x", 64), wire.Sink()).len > 0
    check p.argBuf.len <= 32
    p = wire.initParser(maxPayload = 128)
    check p.parse("MSG x 1 129\r\n", wire.Sink()).len > 0
    check p.msgBuf.len == 0

  test "malformed random bytes and split boundaries never raise Defects":
    var rng = initRand(901)
    for trial in 0 ..< 2000:
      var p = wire.initParser(maxControlLine = 64, maxPayload = 256)
      var text = if trial mod 2 == 0: "MSG " else: "INFO "
      for k in 0 ..< 100: text.add(char(rng.rand(255)))
      for ch in text:
        if p.parse($ch, wire.Sink()).len > 0: break

  test "URL errors never echo credentials; credentials are percent-decoded":
    for url in ["tls://user:SECRET@host:4222", "nats://user:SECRET@host:bad",
                "nats://user:SECRET@[::1]:4222", "nats://user:SECRET@host:0"]:
      try:
        discard core.parseUrl(url)
        check false
      except core.NatsError as e: check not e.msg.contains("SECRET")
    let p = core.parseUrl("nats://u%40x:p%3A%2B@127.0.0.1:4222")
    check p.user == "u@x"
    check p.pass == "p:+"
    var opts = core.defaultDialOptions()
    opts.reconnectJitterMs = -1
    expect core.NatsError: discard core.dial("127.0.0.1:1", opts)

  test "NUID ranges are half-open, including rollover":
    for seed in 1'i64 .. 1000'i64:
      var n = newNuID(seed)
      check n.seq >= 0 and n.seq < maxSeq
      check n.inc >= minInc and n.inc < maxInc
      n.seq = maxSeq
      discard n.next()
      check n.seq < maxSeq and n.inc < maxInc

proc allocationRound(url: string, n: int) =
  var nc = connect(url)
  defer: nc.close()
  var sub: ptr natsSubscription
  doAssert natsConnection_SubscribeSync(addr sub, nc.conn, "memory") == NATS_OK
  defer: natsSubscription_Destroy(sub)
  doAssert natsConnection_Flush(nc.conn) == NATS_OK
  let body = repeat("x", 1024)
  for i in 0 ..< n:
    nc.publish("memory", body)
    var msg: ptr natsMsg
    doAssert natsSubscription_NextMsg(addr msg, sub, 1000) == NATS_OK
    natsMsg_Destroy(msg)

proc runBusTests() =
  let srv = startServer(maxPayload = 4096)
  defer: srv.stop()
  suite "ownership and message semantics":
    test "destroy frees handles and their managed fields":
      allocationRound(srv.url, 20)
      GC_fullCollect()
      let before = getOccupiedMem()
      allocationRound(srv.url, 2000)
      GC_fullCollect()
      check getOccupiedMem() - before < 256 * 1024

    test "out-parameters are validated and reset on failure":
      var nc = connect(srv.url)
      defer: nc.close()
      check natsConnection_SubscribeSync(nil, nc.conn, "x") == NATS_ERR
      var sub = cast[ptr natsSubscription](1)
      check natsConnection_SubscribeSync(addr sub, nc.conn, "a b") == NATS_ERR
      check sub == nil
      var msg = cast[ptr natsMsg](1)
      check natsSubscription_NextMsg(addr msg, nil, 0) == NATS_ERR
      check msg == nil
      check natsConnection_Publish(nc.conn, "x", nil, 1) == NATS_ERR
      check natsConnection_Publish(nc.conn, "x", "", -1) == NATS_ERR
      natsMsg_Destroy(nil)
      natsSubscription_Destroy(nil)
      natsConnection_Destroy(nil)

    test "application status bodies and wire ordering are preserved":
      let c = core.dial(srv.url)
      defer: c.close()
      let s = c.subscribe("status")
      let headers = "NATS/1.0 503 App-status\r\n\r\n"
      for body in ["first", "", "third"]:
        c.deliver(wire.MsgArgs(subject: "status", sid: s.sid,
          hdr: headers.len, size: headers.len + body.len), headers & body)
      check s.nextMsg(0).data == "first"
      expect core.NoRespondersError: discard s.nextMsg(0)
      check s.nextMsg(0).data == "third"
      check c.pendingBytes == 0

    test "byte limits apply before allocation and are recoverable":
      var opts = core.defaultDialOptions()
      opts.maxPendingBytes = 600
      let c = core.dial(srv.url, opts)
      defer: c.close()
      let a = c.subscribe("a", pendingBytes = 400)
      let b = c.subscribe("b", pendingBytes = 400)
      let payload = repeat("x", 200)
      c.deliver(wire.MsgArgs(subject: "a", sid: a.sid, hdr: -1), payload)
      c.deliver(wire.MsgArgs(subject: "a", sid: a.sid, hdr: -1), payload)
      check a.dropped == 1
      c.deliver(wire.MsgArgs(subject: "b", sid: b.sid, hdr: -1), payload)
      check b.dropped == 1  # aggregate connection cap
      expect core.NatsError: discard a.nextMsg(0)
      check a.nextMsg(0).data == payload
      check c.pendingBytes == 0
      expect core.NatsError: discard b.nextMsg(0)
      c.deliver(wire.MsgArgs(subject: "b", sid: b.sid, hdr: -1), payload)
      check b.nextMsg(0).data == payload
      check c.pendingBytes == 0

    test "auto-unsubscribe keeps exactly the requested queued messages":
      let c = core.dial(srv.url)
      defer: c.close()
      let s = c.subscribe("auto")
      s.unsubscribe(2)
      for i in 0 ..< 3: c.publish("auto", $i)
      c.flush()
      check s.nextMsg(0).data == "0"
      check s.nextMsg(0).data == "1"
      check s.closed
      check c.subscriptionCount == 0
      check c.pendingBytes == 0
      expect core.NatsError: discard s.nextMsg(0)
      s.unsubscribe() # explicit release is still harmless

    test "publishes are on the wire when publish returns (cross-connection)":
      # Go's flusher goroutine makes publishes visible to other connections
      # without further calls; this design has no thread, so publish writes
      # through. Regression for a batching attempt that deadlocked
      # "publish on A, then read on B" patterns (t_shim's binary test).
      let a = core.dial(srv.url)
      defer: a.close()
      let b = core.dial(srv.url)
      defer: b.close()
      let sub = b.subscribe("xconn")
      b.flush()
      a.publish("xconn", "cross")
      check sub.nextMsg(2000).data == "cross"

    test "publish/subscribe wire order is preserved":
      let c = core.dial(srv.url)
      defer: c.close()
      c.publish("order", "early")
      let s2 = c.subscribe("order")
      c.publish("order", "late")
      c.flush()
      check s2.nextMsg(1000).data == "late"
      expect core.NatsTimeout:
        discard s2.nextMsg(50)

    test "interleaved subjects deliver with correct subjects (token reuse)":
      # The parser reuses the subject allocation when bytes match; alternating
      # subjects must never leak one subject into another message.
      let c = core.dial(srv.url)
      defer: c.close()
      let a = c.subscribe("subj.a")
      let b = c.subscribe("subj.b")
      c.flush()
      for i in 0 ..< 50:
        c.publish("subj.a", "a" & $i)
        c.publish("subj.b", "b" & $i)
      c.flush()
      for i in 0 ..< 50:
        check a.nextMsg(1000).data == "a" & $i
        check b.nextMsg(1000).data == "b" & $i

    test "a publish burst arrives complete and in order":
      let c = core.dial(srv.url)
      defer: c.close()
      let s = c.subscribe("burst")
      c.flush()
      for i in 0 ..< 1000:
        c.publish("burst", $i)
      var got = 0
      while got < 1000:
        check s.nextMsg(2000).data == $got
        inc got

    test "simultaneous server fixtures do not share directories or ports":
      let other = startServer()
      defer: other.stop()
      check srv.dir != other.dir
      check srv.port != other.port

  suite "reconnect regressions":
    test "exhausted attempts reject publishing; zero polls stay bounded":
      var bus = startServer()
      defer: bus.stop()
      var opts = core.defaultDialOptions()
      opts.maxReconnects = 1
      opts.reconnectWaitMs = 0
      opts.reconnectJitterMs = 0
      let c = core.dial(bus.url, opts)
      defer: c.close()
      let s = c.subscribe("exhaust")
      c.flush()
      bus.stopServerProcess()
      for i in 0 ..< 20:
        let t0 = getMonoTime()
        try: discard s.nextMsg(0)
        except core.NatsError: discard
        check (getMonoTime() - t0).inMilliseconds < 100
        if not c.connected and c.reconnectAttempts == 1: break
      # Finish the pending connect without starting any additional attempt.
      try: discard s.nextMsg(50)
      except core.NatsError: discard
      check c.reconnectAttempts == 1
      expect core.NatsError: c.publish("exhaust", "lost")
      check c.bufferedBytes == 0

    test "retry due time is stable under repeated nonblocking probes":
      var bus = startServer()
      defer: bus.stop()
      var opts = core.defaultDialOptions()
      opts.reconnectWaitMs = 1000
      opts.reconnectJitterMs = 1000
      let c = core.dial(bus.url, opts)
      defer: c.close()
      let s = c.subscribe("jitter")
      c.flush()
      bus.stopServerProcess()
      try: discard s.nextMsg(1)
      except core.NatsError: discard
      check not c.connected
      let due = c.reconnectDueInMs
      check due >= 900 and due <= 2000
      let t0 = getMonoTime()
      for i in 0 ..< 1000: discard c.connectionAlive()
      let elapsed = (getMonoTime() - t0).inMilliseconds.int
      check c.reconnectDueInMs <= due
      check c.reconnectDueInMs >= due - elapsed - 2
      check c.reconnectAttempts == 0

    test "auto-unsubscribe remaining count survives reconnect":
      var bus = startServer()
      defer: bus.stop()
      var opts = core.defaultDialOptions()
      opts.reconnectWaitMs = 0
      opts.reconnectJitterMs = 0
      let c = core.dial(bus.url, opts)
      defer: c.close()
      let s = c.subscribe("auto.restart")
      s.unsubscribe(3)
      c.publish(s.subject, "before")
      check s.nextMsg(1000).data == "before"
      bus.restart()
      for i in 0 ..< 30:
        try: discard s.nextMsg(20)
        except core.NatsError: discard
        if c.reconnectCount > 0: break
      check c.reconnectCount == 1
      s.unsubscribe(3) # updating the limit uses the current server's base count
      for i in 0 ..< 3: c.publish(s.subject, $i)
      c.flush()
      check s.nextMsg(0).data == "0"
      check s.nextMsg(0).data == "1"
      check s.closed

if serverAvailable(): runBusTests()
else: skipBanner()
