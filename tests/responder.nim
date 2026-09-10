## Test fixture: an echo responder process.
##
## `request()` and the shim's `natsConnection_Request` block while waiting for a
## reply, so a single-threaded test cannot service the responder itself. This
## tiny program is that other party: it subscribes and answers, letting the
## tests exercise the real request/reply path end to end.
##
##   responder <url> <subject> [prefix] [--once] [--ready <path>]
##
## It answers each message on its reply subject with `prefix & <body>`, exits
## after one message when `--once` is given, and touches <path> once it is
## subscribed (a file, not a line on stdout: the test polls for it without
## needing a read timeout).

import std/[os, strutils]
import nats/conn as core

proc main() =
  let args = commandLineParams()
  if args.len < 2:
    stderr.writeLine("usage: responder <url> <subject> [prefix] [--once]")
    quit(2)
  let url = args[0]
  let subject = args[1]
  var prefix = ""
  var once = false
  var readyPath = ""
  var i = 2
  while i < args.len:
    if args[i] == "--once": once = true
    elif args[i] == "--ready" and i + 1 < args.len:
      readyPath = args[i + 1]
      inc i
    else: prefix = args[i]
    inc i

  let c = core.dial(url)
  let sub = c.subscribe(subject)
  c.flush()
  if readyPath.len > 0: writeFile(readyPath, "ready\n")
  echo "READY"
  stdout.flushFile()
  while true:
    var m: core.Message
    try:
      m = sub.nextMsg(30000)
    except core.NatsTimeout:
      quit(3)          # nothing to do for 30s: give up
    except core.NatsError:
      quit(4)
    if m.reply.len > 0:
      c.publish(m.reply, prefix & m.data)
      c.flush()
    if once:
      c.close()
      quit(0)

main()
