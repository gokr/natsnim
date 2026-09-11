## Minimal role-based bench peer for head-to-head client comparison.
## Roles: resp | req | sub | pub | dial  (see run.py for pairing).
import std/[os, strutils, monotimes, times]
import natsnim/conn as core

proc main() =
  let role = paramStr(1)
  let url = paramStr(2)
  case role
  of "resp":
    let c = core.dial(url)
    let sub = c.subscribe(paramStr(3))
    c.flush()
    echo "READY"; stdout.flushFile()
    while true:
      let m = sub.nextMsg(30000)
      if m.reply.len > 0: c.publish(m.reply, m.data)
  of "req":
    let subject = paramStr(3)
    let count = parseInt(paramStr(4))
    let payload = repeat("x", parseInt(paramStr(5)))
    let c = core.dial(url)
    let t0 = getMonoTime()
    for i in 1 .. count:
      discard c.request(subject, payload, 5000)
    echo "ELAPSED_US ", (getMonoTime() - t0).inMicroseconds
  of "sub":
    let c = core.dial(url)
    let sub = c.subscribe(paramStr(3))
    c.flush()
    echo "READY"; stdout.flushFile()
    let count = parseInt(paramStr(4))
    var t0: MonoTime
    var n = 0
    while n < count:
      discard sub.nextMsg(60000)
      if n == 0: t0 = getMonoTime()
      inc n
    echo "RECV_US ", (getMonoTime() - t0).inMicroseconds
  of "pub":
    let c = core.dial(url)
    let count = parseInt(paramStr(4))
    let payload = repeat("x", parseInt(paramStr(5)))
    for i in 1 .. count:
      c.publish(paramStr(3), payload)
    c.flush()
    echo "PUB_DONE"
  of "pubbatch":
    # Same as "pub" but with the explicit batch API: N publishes, one send.
    let c = core.dial(url)
    let count = parseInt(paramStr(4))
    let payload = repeat("x", parseInt(paramStr(5)))
    c.deferFlush()
    for i in 1 .. count:
      c.publish(paramStr(3), payload)
    c.flushOutbound()
    echo "PUB_DONE"
  of "dial":
    let count = parseInt(paramStr(3))
    let t0 = getMonoTime()
    for i in 1 .. count:
      let c = core.dial(url)
      c.close()
    echo "ELAPSED_US ", (getMonoTime() - t0).inMicroseconds
  else: quit(2)
main()
