## Shim tests: the `natswrapper`-shaped surface Niffler's Nim components call.
##
## These are the compatibility guarantee — the names and signatures that let
## `sdk/niffler/sdk.nim` switch to this library with a `requires` change and
## no edits. Upstream (nats.c / natswrapper) has no equivalent Nim test, and
## two properties here are easy to get wrong in a translation:
##
##   * `data` + `dataLen` arguments are **binary-safe** (a payload may contain
##     NUL bytes, so it cannot go through `$cstring`),
##   * a timeout is a *status* (`NATS_TIMEOUT`), not an error, while a real
##     failure is `NATS_ERR` with a message from `getErrorString`.

import std/[os, osproc, strutils, times, unittest]
import natsnim
import busharness

proc bytesOf(msg: ptr natsMsg): string =
  ## Reconstruct the payload via its length: `$cstring` stops at the first NUL,
  ## so binary content must be copied with the reported size.
  let n = natsMsg_GetDataLength(msg).int
  result = newString(n)
  if n > 0:
    copyMem(addr result[0], natsMsg_GetData(msg), n)

proc runTests(url: string) =

  suite "natswrapper-compatible shim":

    test "nats_Open/checkStatus/getErrorString basics":
      check nats_Open(-1) == NATS_OK
      check checkStatus(NATS_OK)
      check not checkStatus(NATS_TIMEOUT)
      check getErrorString(NATS_OK) == "ok"
      check getErrorString(NATS_TIMEOUT) == "timeout"
      nats_Close()

    test "connect, subscribe, publish, NextMsg, accessors, destroy":
      var nc = connect(url)
      defer: nc.close()
      var sub: ptr natsSubscription
      check natsConnection_SubscribeSync(addr sub, nc.conn, "shim.t") == NATS_OK
      check natsConnection_FlushTimeout(nc.conn, 2000) == NATS_OK
      check natsConnection_PublishString(nc.conn, "shim.t", "payload") == NATS_OK
      # the length-based publish is the binary-safe variant; a text payload
      # must produce identical bytes either way
      check natsConnection_Publish(nc.conn, "shim.t", "payload2".cstring,
                                   8.cint) == NATS_OK
      var msg: ptr natsMsg
      check natsSubscription_NextMsg(addr msg, sub, 2000) == NATS_OK
      check $natsMsg_GetSubject(msg) == "shim.t"
      check $natsMsg_GetReply(msg) == ""
      check natsMsg_GetDataLength(msg).int == 7
      check $natsMsg_GetData(msg) == "payload"
      natsMsg_Destroy(msg)
      check natsConnection_GetMaxPayload(nc.conn) == 1024
      natsSubscription_Destroy(sub)

    test "natsConnection_Flush does a PING/PONG round trip":
      var nc = connect(url)
      defer: nc.close()
      check natsConnection_Flush(nc.conn) == NATS_OK
      # ...and works as a barrier: what was published before it is deliverable
      var sub: ptr natsSubscription
      check natsConnection_SubscribeSync(addr sub, nc.conn, "shim.flush") == NATS_OK
      check natsConnection_Flush(nc.conn) == NATS_OK
      check natsConnection_PublishString(nc.conn, "shim.flush", "x") == NATS_OK
      check natsConnection_Flush(nc.conn) == NATS_OK
      var msg: ptr natsMsg
      check natsSubscription_NextMsg(addr msg, sub, 0) == NATS_OK
      check $natsMsg_GetData(msg) == "x"
      natsSubscription_Destroy(sub)

    test "QueueSubscribeSync shares one message across the group":
      var nc = connect(url)
      defer: nc.close()
      var s1, s2: ptr natsSubscription
      check natsConnection_QueueSubscribeSync(addr s1, nc.conn, "shim.q",
                                              "workers") == NATS_OK
      check natsConnection_QueueSubscribeSync(addr s2, nc.conn, "shim.q",
                                              "workers") == NATS_OK
      check natsConnection_FlushTimeout(nc.conn, 2000) == NATS_OK
      check natsConnection_PublishString(nc.conn, "shim.q", "once") == NATS_OK
      check natsConnection_FlushTimeout(nc.conn, 2000) == NATS_OK
      var
        m1, m2: ptr natsMsg
        got = 0
      if natsSubscription_NextMsg(addr m1, s1, 500) == NATS_OK: inc got
      if natsSubscription_NextMsg(addr m2, s2, 500) == NATS_OK: inc got
      check got == 1                     # exactly one member got it
      natsSubscription_Destroy(s1)
      natsSubscription_Destroy(s2)

    test "NextMsg reports a timeout as NATS_TIMEOUT, not as an error":
      var nc = connect(url)
      defer: nc.close()
      var sub: ptr natsSubscription
      check natsConnection_SubscribeSync(addr sub, nc.conn, "shim.idle") == NATS_OK
      check natsConnection_FlushTimeout(nc.conn, 2000) == NATS_OK
      var msg: ptr natsMsg
      check natsSubscription_NextMsg(addr msg, sub, 0) == NATS_TIMEOUT
      natsSubscription_Destroy(sub)

    test "PublishRequest/NextMsg carry binary payloads (NUL bytes)":
      # Two connections: a responder would have to be pumped, so drive the
      # request/reply mechanics directly — that is also what proves the
      # cstring+length path is binary-safe.
      var requester = connect(url)
      var responder = connect(url)
      defer:
        requester.close()
        responder.close()
      var svc: ptr natsSubscription
      check natsConnection_SubscribeSync(addr svc, responder.conn,
                                         "shim.bin") == NATS_OK
      let inbox = "_INBOX.shimtest." & $getCurrentProcessId() & "." &
                  $int(epochTime() * 1000)
      var rep: ptr natsSubscription
      check natsConnection_SubscribeSync(addr rep, requester.conn,
                                         inbox) == NATS_OK
      check natsConnection_FlushTimeout(responder.conn, 2000) == NATS_OK
      check natsConnection_FlushTimeout(requester.conn, 2000) == NATS_OK

      let binary = "\x00\x01\x02\xff\x00tail"
      check natsConnection_PublishRequest(requester.conn, "shim.bin",
                                          inbox, binary.cstring,
                                          binary.len.cint) == NATS_OK

      var req: ptr natsMsg
      check natsSubscription_NextMsg(addr req, svc, 2000) == NATS_OK
      check bytesOf(req) == binary          # NULs survived the envelope
      check $natsMsg_GetReply(req) == inbox
      let replySubject = $natsMsg_GetReply(req)
      natsMsg_Destroy(req)

      # answer, then read it back on the requester side. The reply must use
      # the length-based publish: PublishString is NUL-terminated.
      let reply = "\x00reply\x00"
      check natsConnection_Publish(responder.conn, replySubject,
                                   reply.cstring, reply.len.cint) == NATS_OK
      var got: ptr natsMsg
      check natsSubscription_NextMsg(addr got, rep, 2000) == NATS_OK
      check bytesOf(got) == "\x00reply\x00"
      natsMsg_Destroy(got)
      natsSubscription_Destroy(rep)
      natsSubscription_Destroy(svc)

    test "natsConnection_Request end to end (responder process)":
      let resp = startResponder(url, "shim.svc", "echo:")
      defer: stopResponder(resp)
      var nc = connect(url)
      defer: nc.close()
      let payload = "req\x00with\x00nul"
      var msg: ptr natsMsg
      check natsConnection_Request(addr msg, nc.conn, "shim.svc",
                                   payload.cstring, payload.len.cint,
                                   4000) == NATS_OK
      check bytesOf(msg) == "echo:req\x00with\x00nul"
      natsMsg_Destroy(msg)

    test "natsConnection_Request with no responder times out":
      var nc = connect(url)
      defer: nc.close()
      var msg: ptr natsMsg
      let body = "x"
      check natsConnection_Request(addr msg, nc.conn, "shim.nobody",
                                   body.cstring, body.len.cint,
                                   250) == NATS_TIMEOUT
      check getErrorString(NATS_TIMEOUT) == "timeout"

    test "failures report NATS_ERR with a message":
      var nc = connect(url)
      defer: nc.close()
      check natsConnection_PublishString(nc.conn, "bad subject", "x") == NATS_ERR
      check getErrorString(NATS_ERR).len > 0
      var sub: ptr natsSubscription
      check natsConnection_SubscribeSync(addr sub, nc.conn, "a..b") == NATS_ERR
      check getErrorString(NATS_ERR).len > 0
      # a failed subscribe must not hand back a handle to use
      check sub == nil

    test "connecting to a dead port raises IOError, as natswrapper does":
      var raised = false
      try:
        var nc = connect("nats://127.0.0.1:1")
        nc.close()
      except IOError:
        raised = true
      check raised

proc main() =
  if not serverAvailable():
    skipBanner()
    quit(0)
  let srv = startServer(maxPayload = 1024)
  defer: srv.stop()
  runTests(srv.url)

main()
