## Client half of deterministic fake-peer regression tests (t_faultpeer.py).
import std/[monotimes, os, strutils, times]
import natsnim/conn as core

proc fdCount(): int =
  when defined(linux):
    for kind, path in walkDir("/proc/self/fd"): inc result

proc main() =
  let mode = paramStr(1)
  let url = paramStr(2)
  var opts = core.defaultDialOptions()
  opts.handshakeTimeoutMs = 400
  opts.reconnectWaitMs = 0
  opts.reconnectJitterMs = 0
  opts.writeTimeoutMs = 100
  if mode == "failed-dial":
    opts.handshakeTimeoutMs = 20
    let before = fdCount()
    for i in 0 ..< 10:
      var failed = false
      try:
        let c = core.dial(url, opts)
        c.close()
      except core.NatsError: failed = true
      doAssert failed
    GC_fullCollect()
    when defined(linux): doAssert fdCount() == before, "failed dial leaked fd"
    return
  if mode == "auth":
    var failed = false
    try:
      let c = core.dial(url, opts)
      c.close()
    except core.NatsError: failed = true
    doAssert failed
    return
  let c = core.dial(url, opts)
  defer: c.close()
  case mode
  of "error":
    c.publish("denied", "secret")
    var failed = false
    try: c.flush(300)
    except core.NatsError as e:
      failed = true
      doAssert e.msg.contains("Permissions Violation")
    doAssert failed
    doAssert c.connected
    doAssert c.serverErrors.len <= 32
    discard c.takeErrors()
    c.flush(300)
  of "deadline":
    let sub = c.subscribe("t")
    sleep(70)
    try: discard c.pump(0)
    except core.NatsError: discard
    let t0 = getMonoTime()
    try: discard sub.nextMsg(5)
    except core.NatsError: discard
    doAssert (getMonoTime()-t0).inMilliseconds < 100
    # A sequence of short polls must finish the SAME handshake, not restart
    # it on every timeout. The peer delays INFO for 120ms.
    for i in 0 ..< 400:
      try: discard sub.nextMsg(1)
      except core.NatsError: discard
      if c.connected: break
    doAssert c.connected and c.reconnectCount == 1
  of "status0":
    let sub = c.subscribe("t")
    c.flush(300)
    c.publish("kick", "")
    sleep(50)
    var failed = false
    try: discard sub.nextMsg(0)
    except core.NoRespondersError: failed = true
    doAssert failed, "first zero poll must see the 503 it just read"
  of "write":
    let data = repeat("x", 8*1024*1024)
    let t0 = getMonoTime()
    var failed = false
    try: discard c.request("t", data, 20)
    except core.NatsError: failed = true
    doAssert failed
    doAssert (getMonoTime()-t0).inMilliseconds < 250, "write exceeded deadline"
    doAssert c.subscriptionCount == 0
  of "partial":
    var data = newString(4*1024*1024)
    for i in 0 ..< data.len: data[i] = char(i mod 251)
    doAssert c.request("t", data, 3000).data == "intact"
  of "ping":
    let sub = c.subscribe("t")
    doAssert sub.nextMsg(1000).data == "pong-seen"
  of "malformed":
    let sub = c.subscribe("t")
    var failed = false
    try: discard sub.nextMsg(300)
    except core.NatsError: failed = true
    doAssert failed and not c.connected
  else: doAssert false, "unknown mode"
main()
