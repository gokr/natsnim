## Diagnostic only: do not put hardware-sensitive latency thresholds in CI.
import std/[monotimes, times]
import natsnim/conn
import busharness

if not serverAvailable():
  skipBanner()
  quit(1)
let srv = startServer()
defer: srv.stop()
let c = dial(srv.url)
defer: c.close()
let start = getMonoTime()
for i in 0 ..< 1000:
  try: discard c.request("bench.absent", "x", 1000)
  except NoRespondersError: discard
let elapsed = (getMonoTime() - start).inMicroseconds
echo "1000 local no-responder requests: ", elapsed, " us (", elapsed div 1000, " us/request)"
