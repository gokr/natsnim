## Core NATS client: a synchronous, single-threaded connection.
##
## Structure follows `nats-io/nats.go` @ 1ffb90b (Apache-2.0); the transport is
## **rewritten, not translated**, because Go's reader goroutine + channels +
## mutexes have no counterpart in this design (see ASSESSMENT.md):
##
##   - nothing runs in the background. The socket is read only inside
##     `nextMsg` / `flush` / `request` / `pump`, with poll(2) supplying the
##     timeout, and the parser demultiplexes every incoming `MSG` into the
##     target subscription's queue.
##   - therefore a `Connection` is **not safe for concurrent use from several
##     threads**; a worker thread owns its own connection.
##   - `unsubscribe` discards locally queued messages (Go's `Unsubscribe`
##     semantics); `nextMsg` on a closed subscription raises.
##
## Supported: plaintext TCP, optional user/pass, queue groups, headers (HMSG
## parsed and split into `headers`/`data`), request/reply with inboxes,
## `max_payload` enforcement. Not supported, and documented as such in the
## README: TLS, nkeys/JWT, WebSocket, automatic reconnect/backoff (a dropped
## connection fails the calls in flight; resubscription is the caller's).

import std/[deques, json, monotimes, nativesockets, net, os, strutils, tables, times]
from std/posix import poll, TPollfd, Tnfds, POLLIN, POLLERR, POLLHUP, POLLNVAL

import nats/parser
import nats/nuid
import nats/subject

const
  defaultConnectTimeoutMs* = 5000
  defaultHandshakeTimeoutMs* = 10000
  defaultFlushTimeoutMs* = 5000
  defaultPendingLimit* = 65536
    ## Messages buffered per subscription before it is marked overrun. Go's
    ## default is 500_000 msgs with a silent drop plus an async error callback;
    ## this client has no async error channel, so the overflow is recorded and
    ## surfaced (fail-loud) by the next `nextMsg`.
  readChunk = 65536

type
  NatsError* = object of CatchableError
  NatsTimeout* = object of NatsError

  ServerInfo* = object
    serverId*: string
    serverName*: string
    version*: string
    maxPayload*: int
    headers*: bool
    authRequired*: bool
    proto*: int
    raw*: JsonNode

  Message* = ref object
    subject*: string
    reply*: string
    data*: string      ## body: the payload, or the payload after the header block
    headers*: string   ## raw header block ("" when the message carries none)
    sid*: int64
    size*: int         ## full payload size as framed

  Subscription* = ref object
    conn*: Connection
    subject*: string
    queue*: string
    sid*: int64
    msgs: Deque[Message]
    pendingLimit*: int
    dropped*: int64    ## messages discarded because the limit was hit
    overrun*: bool     ## set once `dropped > 0`
    closed*: bool

  Connection* = ref object
    sock: Socket
    readBuf: string
    parser: Parser
    sink: Sink
    subs: Table[int64, Subscription]
    info*: ServerInfo
    maxPayload*: int
    sidSeq: int64
    closed*: bool
    serverErrors*: seq[string]
    pingsOut: int64
    pongsIn: int64

  DialOptions* = object
    connectTimeoutMs*: int
    handshakeTimeoutMs*: int
    clientName*: string

proc defaultDialOptions*(): DialOptions =
  DialOptions(connectTimeoutMs: defaultConnectTimeoutMs,
              handshakeTimeoutMs: defaultHandshakeTimeoutMs)

proc fail(msg: string) {.noreturn.} =
  raise newException(NatsError, msg)

proc pendingCount*(sub: Subscription): int =
  ## Messages queued locally for `sub` and not yet read.
  sub.msgs.len

proc subscriptionCount*(conn: Connection): int =
  ## Live subscriptions on this connection (used by tests to prove that a
  ## timed-out `request` leaves no inbox subscription behind).
  conn.subs.len

# --- parser sink ------------------------------------------------------------

proc deliver*(conn: Connection, a: MsgArgs, payload: string) =
  ## Route one parsed message to its subscription. Public because the parser's
  ## sink needs it (and because it is the seam the pending-limit test drives
  ## without a server).
  let sub = conn.subs.getOrDefault(a.sid)
  if sub == nil:
    return   # an unsubscribe raced the message: dropped, as upstream does
  var m = Message(subject: a.subject, reply: a.reply, sid: a.sid,
                  size: payload.len)
  if a.hdr >= 0:
    m.headers = payload[0 ..< a.hdr]
    m.data = payload[a.hdr .. ^1]
  else:
    m.data = payload
  if sub.pendingLimit > 0 and sub.msgs.len >= sub.pendingLimit:
    inc sub.dropped
    sub.overrun = true
    return
  sub.msgs.addLast(m)

# --- low-level transport ----------------------------------------------------

proc waitReadable(conn: Connection, timeoutMs: int): bool =
  ## poll(2) for readability. `timeoutMs < 0` waits forever.
  var fds: array[1, TPollfd]
  fds[0].fd = cint(conn.sock.getFd())
  fds[0].events = POLLIN
  fds[0].revents = 0
  let t = if timeoutMs < 0: -1.cint else: cint(timeoutMs)
  let r = poll(addr fds[0], 1.Tnfds, t)
  if r < 0:
    fail("poll: " & $osLastError())
  if r == 0:
    return false
  if (fds[0].revents and (POLLERR or POLLHUP or POLLNVAL)) != 0:
    return true   # let recv report the detail
  (fds[0].revents and POLLIN) != 0

proc writeAll(conn: Connection, data: string) =
  if conn.sock == nil: fail("connection has no socket")
  try:
    conn.sock.send(data)
  except CatchableError as e:
    fail("send: " & e.msg)

proc feed(conn: Connection, data: string) =
  let e = conn.parser.parse(data, conn.sink)
  if e.len > 0: fail(e)

proc readSome(conn: Connection): int =
  ## One recv, fed straight to the parser (whose sink queues messages).
  var buf = ""
  try:
    buf = conn.sock.recv(readChunk)
  except CatchableError as e:
    fail("recv: " & e.msg)
  if buf.len == 0:
    conn.closed = true
    fail("connection closed by server")
  conn.feed(buf)
  buf.len

proc pump*(conn: Connection, timeoutMs: int): bool =
  ## Wait up to `timeoutMs` ms for bytes and parse them; false when none came.
  if conn.closed: fail("connection is closed")
  if not conn.waitReadable(timeoutMs): return false
  discard conn.readSome()
  true

proc applyInfo(conn: Connection, raw: JsonNode) =
  ## Install/refresh server info. Called from the parser's `onInfo`, so an
  ## async INFO (a server reload) updates max_payload too, as upstream does.
  conn.info.raw = raw
  conn.info.serverId = raw{"server_id"}.getStr("")
  conn.info.serverName = raw{"server_name"}.getStr("")
  conn.info.version = raw{"version"}.getStr("")
  conn.info.maxPayload = raw{"max_payload"}.getInt(0)
  conn.info.headers = raw{"headers"}.getBool(false)
  conn.info.authRequired = raw{"auth_required"}.getBool(false)
  conn.info.proto = raw{"proto"}.getInt(0)
  if conn.info.maxPayload > 0:
    conn.maxPayload = conn.info.maxPayload

proc makeSink(conn: Connection): Sink =
  ## The parser's callbacks, closed over the connection. `onPing` answers the
  ## server's keepalive so an idle connection is not dropped by the server.
  proc onMsg(a: MsgArgs, payload: string) = conn.deliver(a, payload)
  proc onErr(text: string) = conn.serverErrors.add(text)
  proc onPong() = inc conn.pongsIn
  proc onInfo(raw: string) =
    try:
      conn.applyInfo(parseJson(raw))
    except CatchableError as e:
      conn.serverErrors.add("bad INFO: " & e.msg)
  proc onPing() =
    try:
      conn.writeAll("PONG\r\n")
    except CatchableError:
      discard
  Sink(onMsg: onMsg, onErr: onErr, onPong: onPong, onInfo: onInfo,
       onPing: onPing)

proc setSink(conn: Connection) =
  ## Install the parser callbacks (they need the transport procs above).
  conn.sink = makeSink(conn)

# --- URL parsing ------------------------------------------------------------

type UrlParts* = object
  host*: string
  port*: int
  user*: string
  pass*: string

proc parseUrl*(url: string): UrlParts =
  ## Accepts `nats://host:port`, `nats://user:pass@host:port`, `host:port` and
  ## `host` (default port 4222).
  var s = url
  for scheme in ["nats://", "tcp://"]:
    if s.startsWith(scheme):
      s = s[scheme.len .. ^1]
      break
  for i in 0 ..< s.len:
    if s[i] == '/':
      s = s[0 ..< i]
      break
  if s.len == 0: fail("empty server url: '" & url & "'")
  let at = s.rfind('@')
  if at >= 0:
    let cred = s[0 ..< at]
    s = s[at + 1 .. ^1]
    let colon = cred.find(':')
    if colon >= 0:
      result.user = cred[0 ..< colon]
      result.pass = cred[colon + 1 .. ^1]
    else:
      result.user = cred
  # IPv6 in brackets is not supported; a bare host:port is split on the last ':'.
  let c = s.rfind(':')
  if c >= 0 and s.find(':') == c:
    result.host = s[0 ..< c]
    try:
      result.port = parseInt(s[c + 1 .. ^1])
    except ValueError:
      fail("bad port in server url: '" & url & "'")
  else:
    result.host = s
    result.port = 4222
  if result.host.len == 0: fail("no host in server url: '" & url & "'")
  if result.port <= 0 or result.port > 65535:
    fail("bad port in server url: '" & url & "'")

# --- connection lifecycle ---------------------------------------------------

proc connectSocket(parts: UrlParts, timeoutMs: int): Socket =
  ## Two Nim details matter here, and both are load-bearing:
  ##
  ##  * `newSocket()` is *buffered* by default, and a buffered `recv(fd, size)`
  ##    loops until it has filled the whole request (see net.nim's
  ##    `if socket.isBuffered` path). Asking for 64 KiB would therefore block
  ##    after a 6-byte PONG even though poll(2) said "readable". This client
  ##    does its own readiness handling, so the socket must be unbuffered.
  ##  * `connect(..., timeout)` leaves the fd **non-blocking**; a subsequent
  ##    blocking `recv` would then fail with EAGAIN, which the SafeDisconn flag
  ##    reports as 0 = "peer closed". Restore blocking mode and let poll(2)
  ##    decide when a read may happen.
  result = newSocket(buffered = false)
  try:
    result.connect(parts.host, Port(parts.port), timeoutMs)
    setBlocking(result.getFd(), true)
  except CatchableError as e:
    result.close()
    fail("cannot connect to " & parts.host & ":" & $parts.port & ": " & e.msg)

proc dial*(url: string, opts = defaultDialOptions()): Connection =
  ## Connect, complete the INFO/CONNECT handshake and validate it with a
  ## PING/PONG round trip (which is also when an auth failure surfaces).
  let parts = parseUrl(url)
  result = Connection(
    sock: connectSocket(parts, opts.connectTimeoutMs),
    parser: initParser(),
    subs: initTable[int64, Subscription](),
    pongsIn: 0,
    pingsOut: 0,
    serverErrors: @[])
  result.setSink()

  # 1. The server speaks first: INFO.
  let deadline = getMonoTime() + initDuration(milliseconds = opts.handshakeTimeoutMs)
  while result.info.raw == nil:
    if result.serverErrors.len > 0:
      fail("server refused the connection: " & result.serverErrors[^1])
    let rem = (deadline - getMonoTime()).inMilliseconds
    if rem <= 0:
      fail("handshake timed out waiting for INFO")
    if not result.pump(rem.int):
      continue

  # 2. CONNECT.
  var c = %*{
    "verbose": false,
    "pedantic": false,
    "lang": "nim",
    "version": "0.1.0",
    "protocol": 1,
    "echo": true,
    "headers": true,
    "no_responders": false
  }
  if opts.clientName.len > 0: c["name"] = %opts.clientName
  if parts.user.len > 0:
    c["user"] = %parts.user
    c["pass"] = %parts.pass
  result.writeAll("CONNECT " & $c & "\r\n")

  # 3. PING/PONG validation.
  result.writeAll("PING\r\n")
  inc result.pingsOut
  while result.pongsIn < result.pingsOut:
    if result.serverErrors.len > 0:
      fail("server refused the connection: " & result.serverErrors[^1])
    let rem = (deadline - getMonoTime()).inMilliseconds
    if rem <= 0:
      fail("handshake timed out waiting for PONG")
    if not result.pump(rem.int):
      continue

proc close*(conn: Connection) =
  if conn.closed and conn.sock == nil: return
  conn.closed = true
  for sub in conn.subs.values: sub.closed = true
  conn.subs.clear()
  if conn.sock != nil:
    try: conn.sock.close()
    except CatchableError: discard
    conn.sock = nil

# --- subscribe / publish ----------------------------------------------------

proc subscribe*(conn: Connection, subject: string, queue = "",
                pendingLimit = defaultPendingLimit): Subscription =
  if conn.closed: fail("connection is closed")
  if badSubject(subject): fail("invalid subject: '" & subject & "'")
  if queue.len > 0 and badQueue(queue):
    fail("invalid queue name: '" & queue & "'")
  inc conn.sidSeq
  result = Subscription(conn: conn, subject: subject, queue: queue,
                        sid: conn.sidSeq, msgs: initDeque[Message](),
                        pendingLimit: pendingLimit)
  conn.subs[result.sid] = result   # registered before the wire so no reply is lost
  var cmd = "SUB " & subject
  if queue.len > 0: cmd.add(" " & queue)
  cmd.add(" " & $result.sid & "\r\n")
  conn.writeAll(cmd)

proc unsubscribe*(sub: Subscription, maxMsgs = 0) =
  ## Send UNSUB, drop the local queue and detach from the connection.
  if sub.closed: return
  sub.closed = true
  sub.msgs.clear()
  if sub.conn != nil:
    sub.conn.subs.del(sub.sid)
    if not sub.conn.closed:
      var cmd = "UNSUB " & $sub.sid
      if maxMsgs > 0: cmd.add(" " & $maxMsgs)
      cmd.add("\r\n")
      try: sub.conn.writeAll(cmd)
      except CatchableError: discard

proc publishRaw(conn: Connection, subject, reply, data: string) =
  if conn.closed: fail("connection is closed")
  if badSubject(subject): fail("invalid subject: '" & subject & "'")
  if reply.len > 0 and badSubject(reply):
    fail("invalid reply subject: '" & reply & "'")
  if conn.maxPayload > 0 and data.len > conn.maxPayload:
    fail("payload of " & $data.len &
         " bytes exceeds the server max_payload of " & $conn.maxPayload)
  var h = "PUB " & subject
  if reply.len > 0: h.add(" " & reply)
  h.add(" " & $data.len & "\r\n")
  conn.writeAll(h)
  conn.writeAll(data)
  conn.writeAll("\r\n")

proc publish*(conn: Connection, subject, data: string) =
  conn.publishRaw(subject, "", data)

proc publishRequest*(conn: Connection, subject, reply, data: string) =
  ## Publish with a reply subject: the counterpart of Go's `PublishRequest`.
  conn.publishRaw(subject, reply, data)

# --- receiving --------------------------------------------------------------

proc nextMsg*(sub: Subscription, timeoutMs: int): Message =
  ## Block up to `timeoutMs` ms for the next message on `sub`. `0` means "do
  ## not wait" (a non-blocking poll, as upstream). Raises `NatsTimeout` when
  ## nothing arrives, `NatsError` if the subscription or connection is closed
  ## or the pending limit was exceeded.
  if sub.overrun:
    fail("subscription on '" & sub.subject & "' exceeded its pending limit (" &
         $sub.pendingLimit & " msgs); " & $sub.dropped &
         " message(s) were dropped")
  var deadline: MonoTime
  let timed = timeoutMs > 0
  if timed: deadline = getMonoTime() + initDuration(milliseconds = timeoutMs)
  while true:
    if sub.msgs.len > 0: return sub.msgs.popFirst()
    if sub.closed: fail("subscription on '" & sub.subject & "' is closed")
    let conn = sub.conn
    if conn == nil or conn.closed: fail("connection is closed")
    var waitMs = 0
    if timed:
      let rem = (deadline - getMonoTime()).inMilliseconds
      if rem <= 0: break
      waitMs = rem.int
    discard conn.pump(waitMs)
    if sub.msgs.len > 0: return sub.msgs.popFirst()
    if not timed: break
  raise newException(NatsTimeout,
    "no message on '" & sub.subject & "' within " & $timeoutMs & "ms")

proc tryNextMsg*(sub: Subscription, timeoutMs: int): Message =
  ## Convenience wrapper returning nil instead of raising `NatsTimeout`.
  try:
    result = sub.nextMsg(timeoutMs)
  except NatsTimeout:
    result = nil

proc take*(sub: Subscription): Message =
  ## Non-blocking pop of a message already queued (no socket read).
  if sub.msgs.len == 0: return nil
  sub.msgs.popFirst()

proc flush*(conn: Connection, timeoutMs = defaultFlushTimeoutMs) =
  ## PING/PONG round trip. Messages arriving meanwhile are queued to their
  ## subscriptions, never lost.
  if conn.closed: fail("connection is closed")
  conn.writeAll("PING\r\n")
  inc conn.pingsOut
  let deadline = getMonoTime() + initDuration(milliseconds = timeoutMs)
  while conn.pongsIn < conn.pingsOut:
    let rem = (deadline - getMonoTime()).inMilliseconds
    if rem <= 0:
      raise newException(NatsTimeout,
        "flush timed out after " & $timeoutMs & "ms")
    discard conn.pump(rem.int)

proc newInbox*(conn: Connection): string =
  ## A unique reply subject. Uses the ported NUID, as upstream does.
  discard conn
  "_INBOX." & nextId()

proc request*(conn: Connection, subject, data: string,
              timeoutMs = defaultFlushTimeoutMs): Message =
  ## Request/reply: subscribe an inbox, publish with that reply subject, wait
  ## for one message. The inbox subscription is removed before returning.
  let inbox = conn.newInbox()
  let sub = conn.subscribe(inbox)
  let deadline = getMonoTime() + initDuration(milliseconds = timeoutMs)
  try:
    conn.publishRequest(subject, inbox, data)
    let rem = (deadline - getMonoTime()).inMilliseconds
    if rem > 0:
      result = sub.nextMsg(rem.int)
    else:
      result = sub.nextMsg(0)
  finally:
    sub.unsubscribe()
