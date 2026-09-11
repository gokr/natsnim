## Pure-Nim NATS client (core NATS), natswrapper-shaped compatibility API.
##
## Two layers:
##
##   * `natsnim/conn` — the idiomatic Nim API (`Connection`, `Subscription`,
##     `Message`, `dial`, `publish`, `nextMsg`, …). Use that directly.
##   * this module — the **shim contract**: the subset of `natswrapper` (a
##     Futhark FFI binding over `nats.c`) that Niffler's Nim sources use, with
##     the same names and signatures, so adopting this library is a `requires`
##     change and `sdk/niffler/sdk.nim` does not move.
##
## Pointer handles are explicitly owned, like the C API: each successful
## allocation needs exactly one matching Destroy (or close for the convenience
## connection). Copies of NatsConnection are borrowed aliases, not new owners;
## destroying one invalidates every alias. Never use or destroy a freed handle.
##
## Not thread-safe (see README): one connection per thread.

import natsnim/conn as core
export NoRespondersError

export core

type
  natsStatus* = cint
    ## Congruent with `nats.c`'s `natsStatus` (NATS_OK == 0).

  natsConnection* = object
    impl: core.Connection
  natsSubscription* = object
    impl: core.Subscription
  natsMsg* = object
    subject: string
    reply: string
    data: string
    headers: string
    sid: int64

  NatsConnection* = object
    ## natswrapper-shaped convenience handle (kept for drop-in compatibility).
    conn*: ptr natsConnection

const
  NATS_OK* = 0.natsStatus
  NATS_ERR* = 1.natsStatus
  NATS_NO_RESPONDERS* = 23.natsStatus
    ## The server answered a request with 503: no subscriber on the subject.
  NATS_TIMEOUT* = 21.natsStatus
    ## Sentinel for "no message within the timeout". The numeric value is ours
    ## (only `checkStatus` and `== NATS_TIMEOUT` are contractual); nats.c's
    ## internal enum is not part of this library's interface.

var lastErrorText {.threadvar.}: string

proc getErrorString*(status: natsStatus): string =
  ## Status text. For NATS_ERR this is the most recent shim error, mirroring
  ## `nats_GetLastError`. Error state belongs to the calling thread.
  case status
  of NATS_OK: "ok"
  of NATS_TIMEOUT: "timeout"
  of NATS_NO_RESPONDERS: "no responders available for request"
  of NATS_ERR: lastErrorText
  else: "nats status " & $status

proc checkStatus*(status: natsStatus): bool {.inline.} =
  status == NATS_OK

proc lastError*(): string =
  ## Last shim-level error message ("" when none).
  lastErrorText

proc setErr(msg: string): natsStatus {.discardable.} =
  lastErrorText = msg
  NATS_ERR

proc cstrToStr(p: cstring, n: cint): string =
  ## cstring + length: binary-safe (a payload may contain NUL bytes).
  if n < 0 or (p == nil and n > 0):
    raise newException(ValueError, "invalid payload pointer/length")
  if n == 0: return ""
  result = newString(n.int)
  copyMem(addr result[0], p, n.int)

# --- library lifecycle ------------------------------------------------------

proc nats_Open*(sleepMs: int): natsStatus {.discardable.} =
  ## Kept for API compatibility: a pure-Nim client has no global C state to
  ## initialise; inbox generators are connection-owned.
  discard sleepMs
  NATS_OK

proc nats_Close*() =
  discard

# --- connection -------------------------------------------------------------

proc connect*(url: string = "nats://localhost:4222"): NatsConnection =
  ## Connect and complete the INFO/CONNECT handshake. Raises IOError on
  ## failure, as natswrapper's `connect` does.
  try:
    let impl = core.dial(url)
    let handle = create(natsConnection)
    handle.impl = impl
    result.conn = handle
  except CatchableError as e:
    lastErrorText = e.msg
    raise newException(IOError, "Failed to connect: " & e.msg)

proc close*(nc: var NatsConnection) =
  if nc.conn != nil:
    nc.conn.impl.close()
    reset(nc.conn[])
    dealloc(nc.conn)
    nc.conn = nil

proc publish*(nc: NatsConnection, subject: string, data: string) =
  ## Publish, raising IOError on failure (natswrapper semantics).
  if nc.conn == nil:
    raise newException(IOError, "Publish failed: no connection")
  try:
    nc.conn.impl.publish(subject, data)
  except CatchableError as e:
    lastErrorText = e.msg
    raise newException(IOError, "Publish failed: " & e.msg)

proc rawConn(nc: ptr natsConnection): core.Connection =
  if nc == nil or nc.impl == nil: nil else: nc.impl

proc natsConnection_PublishString*(conn: ptr natsConnection,
                                   subject, data: cstring): natsStatus {.discardable.} =
  let c = rawConn(conn)
  if c == nil: return setErr("natsConnection_PublishString: nil connection")
  try:
    c.publish($subject, $data)
    NATS_OK
  except CatchableError as e:
    setErr(e.msg)

proc natsConnection_Publish*(conn: ptr natsConnection, subject: cstring,
                             data: cstring, dataLen: cint): natsStatus {.discardable.} =
  ## Binary-safe publish (data + length), the counterpart of nats.c's
  ## `natsConnection_Publish`. `PublishString` is NUL-terminated, so a payload
  ## containing NUL bytes must go through here.
  let c = rawConn(conn)
  if c == nil: return setErr("natsConnection_Publish: nil connection")
  try:
    c.publish($subject, cstrToStr(data, dataLen))
    NATS_OK
  except CatchableError as e:
    setErr(e.msg)

proc natsConnection_FlushTimeout*(conn: ptr natsConnection,
                                  timeoutMs: int64): natsStatus {.discardable.} =
  let c = rawConn(conn)
  if c == nil: return setErr("natsConnection_FlushTimeout: nil connection")
  try:
    c.flush(timeoutMs.int)
    NATS_OK
  except NatsTimeout as e:
    lastErrorText = e.msg
    NATS_TIMEOUT
  except CatchableError as e:
    setErr(e.msg)

proc natsConnection_Flush*(conn: ptr natsConnection): natsStatus {.discardable.} =
  ## PING/PONG round trip with a bounded wait. Deviation from nats.c, whose
  ## `natsConnection_Flush` blocks until the connection's own default timeout:
  ## this client never waits unbounded (there is no thread to interrupt it), so
  ## it uses `defaultFlushTimeoutMs`.
  let c = rawConn(conn)
  if c == nil: return setErr("natsConnection_Flush: nil connection")
  try:
    c.flush(core.defaultFlushTimeoutMs)
    NATS_OK
  except NatsTimeout as e:
    lastErrorText = e.msg
    NATS_TIMEOUT
  except CatchableError as e:
    setErr(e.msg)

proc natsConnection_GetMaxPayload*(conn: ptr natsConnection): cint =
  let c = rawConn(conn)
  if c == nil: 0.cint else: cint(min(c.maxPayload, high(cint).int))

proc natsConnection_Destroy*(conn: ptr natsConnection) =
  if conn != nil:
    if conn.impl != nil: conn.impl.close()
    reset(conn[])
    dealloc(conn)

# --- subscribe / publish-request -------------------------------------------

proc subscribeSync(conn: ptr natsConnection, subject: string,
                   queue: string): tuple[status: natsStatus,
                                         sub: ptr natsSubscription] =
  let c = rawConn(conn)
  if c == nil: return (setErr("subscribe: nil connection"), nil)
  try:
    let s = c.subscribe(subject, queue)
    let handle = create(natsSubscription)
    handle.impl = s
    (NATS_OK, handle)
  except CatchableError as e:
    (setErr(e.msg), nil)

proc natsConnection_SubscribeSync*(sub: ptr ptr natsSubscription,
                                   conn: ptr natsConnection,
                                   subject: cstring): natsStatus {.discardable.} =
  if sub == nil: return setErr("subscribe: nil output pointer")
  sub[] = nil
  let (st, handle) = subscribeSync(conn, $subject, "")
  if st == NATS_OK: sub[] = handle
  st

proc natsConnection_QueueSubscribeSync*(sub: ptr ptr natsSubscription,
                                        conn: ptr natsConnection,
                                        subject, queue: cstring): natsStatus {.discardable.} =
  if sub == nil: return setErr("subscribe: nil output pointer")
  sub[] = nil
  let (st, handle) = subscribeSync(conn, $subject, $queue)
  if st == NATS_OK: sub[] = handle
  st

proc natsConnection_PublishRequest*(conn: ptr natsConnection,
                                    subject, reply, data: cstring,
                                    dataLen: cint): natsStatus {.discardable.} =
  let c = rawConn(conn)
  if c == nil: return setErr("natsConnection_PublishRequest: nil connection")
  try:
    c.publishRequest($subject, $reply, cstrToStr(data, dataLen))
    NATS_OK
  except CatchableError as e:
    setErr(e.msg)

proc msgHandle(m: core.Message): ptr natsMsg =
  if m == nil: return nil
  let handle = create(natsMsg)
  handle.subject = m.subject
  handle.reply = m.reply
  handle.data = m.data
  handle.headers = m.headers
  handle.sid = m.sid
  handle

proc natsConnection_Request*(msg: ptr ptr natsMsg, conn: ptr natsConnection,
                             subject, data: cstring, dataLen: cint,
                             timeoutMs: int64): natsStatus {.discardable.} =
  if msg == nil: return setErr("request: nil output pointer")
  msg[] = nil
  let c = rawConn(conn)
  if c == nil: return setErr("natsConnection_Request: nil connection")
  try:
    let reply = c.request($subject, cstrToStr(data, dataLen), timeoutMs.int)
    msg[] = msgHandle(reply)
    NATS_OK
  except NoRespondersError as e:
    # nats.c reports this as NATS_NO_RESPONDERS and returns immediately, which
    # callers rely on to distinguish "no such component" from a timeout.
    lastErrorText = e.msg
    NATS_NO_RESPONDERS
  except NatsTimeout as e:
    lastErrorText = e.msg
    NATS_TIMEOUT
  except CatchableError as e:
    setErr(e.msg)

# --- subscription / message -------------------------------------------------

proc natsSubscription_NextMsg*(msg: ptr ptr natsMsg,
                               sub: ptr natsSubscription,
                               timeoutMs: int64): natsStatus {.discardable.} =
  if msg == nil: return setErr("nextMsg: nil output pointer")
  msg[] = nil
  if sub == nil or sub.impl == nil:
    return setErr("natsSubscription_NextMsg: nil subscription")
  try:
    let m = sub.impl.nextMsg(timeoutMs.int)
    msg[] = msgHandle(m)
    NATS_OK
  except NoRespondersError as e:
    lastErrorText = e.msg
    NATS_NO_RESPONDERS
  except NatsTimeout:
    NATS_TIMEOUT
  except CatchableError as e:
    setErr(e.msg)

proc natsSubscription_Unsubscribe*(sub: ptr natsSubscription): natsStatus {.discardable.} =
  if sub == nil or sub.impl == nil:
    return setErr("natsSubscription_Unsubscribe: nil subscription")
  sub.impl.unsubscribe()
  NATS_OK

proc natsSubscription_Destroy*(sub: ptr natsSubscription) =
  ## Unsubscribe and free the handle. Void, as in nats.c. Nil is harmless.
  if sub == nil: return
  if sub.impl != nil: sub.impl.unsubscribe()
  reset(sub[])
  dealloc(sub)

proc natsMsg_GetData*(msg: ptr natsMsg): cstring =
  if msg == nil: "" else: msg.data.cstring

proc natsMsg_GetDataLength*(msg: ptr natsMsg): cint =
  if msg == nil: 0.cint else: cint(msg.data.len)

proc natsMsg_GetSubject*(msg: ptr natsMsg): cstring =
  if msg == nil: "" else: msg.subject.cstring

proc natsMsg_GetReply*(msg: ptr natsMsg): cstring =
  if msg == nil: "" else: msg.reply.cstring

proc natsMsg_GetHeader*(msg: ptr natsMsg): cstring =
  ## Not in nats.c's core API surface we use, but the raw header block is
  ## available; empty when the message carries no headers.
  if msg == nil: "" else: msg.headers.cstring

proc natsMsg_Destroy*(msg: ptr natsMsg) =
  ## Release owned strings and the raw handle. Accessors expire at this call.
  if msg == nil: return
  reset(msg[])
  dealloc(msg)
