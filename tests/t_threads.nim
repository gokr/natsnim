## Each worker owns its connection and inbox generator; no shared mutable
## client state. A main-thread subscription checks uniqueness across workers.
import std/[sets, strutils]
import natsnim
import natsnim/conn as core
import busharness

proc worker(url: string) {.thread.} =
  let c = core.dial(url)
  defer: c.close()
  for i in 0 ..< 20: c.publish("thread.ids", c.newInbox())
  c.flush()
  discard natsConnection_PublishString(nil, "x", "")
  doAssert getErrorString(NATS_ERR).contains("PublishString")

proc main() =
  if not serverAvailable():
    skipBanner()
    return
  let srv = startServer()
  defer: srv.stop()
  let c = core.dial(srv.url)
  defer: c.close()
  let sub = c.subscribe("thread.ids")
  c.flush()
  discard natsSubscription_Unsubscribe(nil)
  let parentError = getErrorString(NATS_ERR)
  var workers: array[4, Thread[string]]
  for i in 0 ..< workers.len: createThread(workers[i], worker, srv.url)
  joinThreads(workers)
  var ids = initHashSet[string]()
  for i in 0 ..< 80: doAssert not ids.containsOrIncl(sub.nextMsg(1000).data)
  doAssert getErrorString(NATS_ERR) == parentError
  echo "OK: independent thread-owned connections and shim error state"
main()
