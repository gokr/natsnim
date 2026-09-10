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

import std/[deques, json, monotimes, nativesockets, net, os, random, strutils,
            tables, times]
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

type UrlParts* = object
  host*: string
  port*: int
  user*: string
  pass*: string

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
    closed*: bool        ## explicitly closed: no reconnecting, all calls fail
    connected*: bool     ## socket is live (false = in a reconnect window)
    handshaking: bool    ## inside a dial/attempt: no nested attempts
    opts: DialOptions
    parts: UrlParts
    haveAttempt: bool
    lastAttempt: MonoTime
    reconnectAttempts*: int
    reconnectCount*: int
    lastDisconnect*: string
    pending: string      ## publishes buffered while disconnected
    rng: Rand
    serverErrors*: seq[string]
    pingsOut: int64
    pongsIn: int64

  DialOptions* = object
    connectTimeoutMs*: int
    handshakeTimeoutMs*: int
    clientName*: string
    reconnectWaitMs*: int
      ## Minimum spacing between reconnect attempts (default 2000).
    maxReconnects*: int
      ## Attempts before giving up (default 60; 0 disables reconnecting;
      ## -1 means unlimited).
    reconnectBufSize*: int
      ## Outgoing publishes buffered while disconnected (default 8 MiB).
      ## Past it a publish fails instead of being dropped, as Go does.
    reconnectDialTimeoutMs*: int
      ## Budget for the *reconnect* dial (default 2000), separate from the
      ## initial `connectTimeoutMs`. A reconnect happens inside a caller
      ## (`nextMsg`/`flush`), so it must not block that caller for as long as
      ## a cold start legitimately may. 0 means "reuse connectTimeoutMs".
    reconnectJitterMs*: int
      ## Random extra [0, jitter) added to `reconnectWaitMs` (default 100), so
      ## that a bus restart does not have every component retrying in lockstep
      ## — the reason nats.go adds jitter.
    reconnect*: bool
      ## Master switch (default true). With it off a lost connection is fatal
      ## to the calls in flight, which is what Niffler did before P5.

proc defaultDialOptions*(): DialOptions =
  DialOptions(connectTimeoutMs: defaultConnectTimeoutMs,
              handshakeTimeoutMs: defaultHandshakeTimeoutMs,
              reconnectWaitMs: 2000, maxReconnects: 60,
              reconnectBufSize: 8 * 1024 * 1024,
              reconnectDialTimeoutMs: 2000, reconnectJitterMs: 100,
              reconnect: true)

proc fail(msg: string) {.noreturn.} =
  raise newException(NatsError, msg)

proc noteDisconnect(conn: Connection, reason: string) =
  ## The connection died. Not an error by itself: it opens a reconnect window
  ## that the *waiting* calls (nextMsg/flush/request) close, since there is no
  ## background thread to do it.
  if not conn.connected: return
  conn.connected = false
  conn.lastDisconnect = reason
  conn.reconnectAttempts = 0
  if conn.sock != nil:
    try: conn.sock.close()
    except CatchableError: discard
    conn.sock = nil


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
    conn.noteDisconnect("poll: " & $osLastError())
    fail("poll: " & $osLastError())
  if r == 0:
    return false
  if (fds[0].revents and (POLLERR or POLLHUP or POLLNVAL)) != 0:
    return true   # let recv report the detail
  (fds[0].revents and POLLIN) != 0

proc writeDirect(conn: Connection, data: string) =
  ## Raw write on the live socket. Only used when connected (handshake,
  ## resubscribe, pending flush, PING/PONG).
  if conn.sock == nil: fail("connection has no socket")
  if conn.connected:
    try:
      conn.sock.send(data)
    except CatchableError as e:
      conn.noteDisconnect("send: " & e.msg)
      fail("send: " & e.msg)
  else:
    # the handshake writes before `connected` is set: allow it, but any
    # failure still has to surface
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
    conn.noteDisconnect("recv: " & e.msg)
    fail("recv: " & e.msg)
  if buf.len == 0:
    conn.noteDisconnect("connection closed by server")
    fail("connection closed by server")
  conn.feed(buf)
  buf.len

proc pump*(conn: Connection, timeoutMs: int): bool =
  ## Wait up to `timeoutMs` ms for bytes and parse them; false when none came.
  ## While disconnected there is nothing to poll, so the timeout is spent
  ## sleeping: a caller pacing on `nextMsg(1)` must not spin hot (and must not
  ## be blocked by a reconnect attempt either — see `ensureConnected`).
  if conn.closed: fail("connection is closed")
  if conn.sock == nil:
    # No socket to poll: spend the timeout so a polling caller (nextMsg(1))
    # keeps its cadence instead of spinning. Note the test is the socket, not
    # `connected` — during a dial/reconnect handshake the socket is live while
    # `connected` is still false.
    if timeoutMs > 0: sleep(timeoutMs)
    return false
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
      conn.writeDirect("PONG\r\n")
    except CatchableError:
      discard
  Sink(onMsg: onMsg, onErr: onErr, onPong: onPong, onInfo: onInfo,
       onPing: onPing)

proc setSink(conn: Connection) =
  ## Install the parser callbacks (they need the transport procs above).
  conn.sink = makeSink(conn)

# --- URL parsing ------------------------------------------------------------

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

proc handshake(conn: Connection) =
  ## INFO → CONNECT → PING/PONG. The server speaks first, and a `-ERR` (an auth
  ## failure, say) surfaces here rather than as a timeout. Used for the initial
  ## dial and for every reconnect attempt, so both paths behave identically.
  let deadline = getMonoTime() +
                 initDuration(milliseconds = conn.opts.handshakeTimeoutMs)

  # 1. INFO (the server's greeting; also re-sent on a reconnect).
  while conn.info.raw == nil:
    if conn.serverErrors.len > 0:
      fail("server refused the connection: " & conn.serverErrors[^1])
    let rem = (deadline - getMonoTime()).inMilliseconds
    if rem <= 0:
      fail("handshake timed out waiting for INFO")
    if not conn.pump(rem.int):
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
  if conn.opts.clientName.len > 0: c["name"] = %conn.opts.clientName
  if conn.parts.user.len > 0:
    c["user"] = %conn.parts.user
    c["pass"] = %conn.parts.pass
  conn.writeDirect("CONNECT " & $c & "\r\n")

  # 3. PING/PONG validation (this is also when an auth failure lands).
  conn.writeDirect("PING\r\n")
  inc conn.pingsOut
  while conn.pongsIn < conn.pingsOut:
    if conn.serverErrors.len > 0:
      fail("server refused the connection: " & conn.serverErrors[^1])
    let rem = (deadline - getMonoTime()).inMilliseconds
    if rem <= 0:
      fail("handshake timed out waiting for PONG")
    if not conn.pump(rem.int):
      continue

proc resubscribe(conn: Connection) =
  ## Re-register every live subscription **with its existing sid**. Keeping the
  ## sid is what makes reconnect transparent: a reply subject handed out before
  ## the outage still routes afterwards, and a subscription's local queue
  ## (messages already delivered) is untouched. This is what Go's
  ## `resubscribe` does, minus the batching its reader goroutine needs.
  for sid, sub in conn.subs:
    if sub.closed: continue
    var cmd = "SUB " & sub.subject
    if sub.queue.len > 0: cmd.add(" " & sub.queue)
    cmd.add(" " & $sid & "\r\n")
    conn.writeDirect(cmd)

proc flushPending(conn: Connection) =
  ## Publishes buffered during the outage, written after the resubscribes so a
  ## message published to one of our own subjects is still delivered to us.
  if conn.pending.len == 0: return
  conn.writeDirect(conn.pending)
  conn.pending.setLen(0)

proc dialTimeoutForAttempt*(opts: DialOptions): int =
  ## The dial budget a *reconnect* attempt gets. Pure, so the decision is
  ## testable without a black-hole network to demonstrate it on.
  if opts.reconnectDialTimeoutMs > 0: opts.reconnectDialTimeoutMs
  else: opts.connectTimeoutMs

proc reconnectDelayMs*(opts: DialOptions, rng: var Rand): int =
  ## The spacing before the next attempt: `reconnectWaitMs` plus jitter drawn
  ## per call, so two components that lost the bus together do not retry in
  ## lockstep. Pure apart from the RNG, so the jitter bounds are testable.
  result = opts.reconnectWaitMs
  if opts.reconnectJitterMs > 0:
    result += rng.rand(opts.reconnectJitterMs - 1)

proc tryReconnect(conn: Connection): bool =
  ## One attempt: dial, handshake, resubscribe, flush buffered publishes.
  ## Never called from a thread — the waiting calls drive it.
  if conn.closed or conn.handshaking or not conn.opts.reconnect: return false
  if conn.opts.maxReconnects == 0: return false
  if conn.opts.maxReconnects > 0 and
     conn.reconnectAttempts >= conn.opts.maxReconnects:
    return false
  conn.haveAttempt = true
  conn.lastAttempt = getMonoTime()
  inc conn.reconnectAttempts
  conn.handshaking = true
  defer: conn.handshaking = false
  try:
    conn.sock = connectSocket(conn.parts, dialTimeoutForAttempt(conn.opts))
    # A fresh parser: a partial frame from the dead socket is meaningless, and
    # a new INFO is on its way.
    conn.parser = initParser()
    conn.info.raw = nil
    conn.serverErrors.setLen(0)
    conn.pongsIn = 0
    conn.pingsOut = 0
    conn.handshake()
    conn.resubscribe()
    conn.flushPending()
    conn.connected = true
    conn.reconnectAttempts = 0
    inc conn.reconnectCount
    true
  except CatchableError as e:
    conn.lastDisconnect = e.msg
    if conn.sock != nil:
      try: conn.sock.close()
      except CatchableError: discard
      conn.sock = nil
    false

proc ensureConnected(conn: Connection): bool =
  ## True when the socket is live. Otherwise maybe attempt a reconnect — but at
  ## most once per `reconnectWaitMs`, so a 1 ms `nextMsg` poll neither spins nor
  ## blocks more than one attempt per window.
  if conn.connected: return true
  if conn.closed or conn.handshaking: return false
  if not conn.opts.reconnect or conn.opts.maxReconnects == 0: return false
  if conn.opts.maxReconnects > 0 and
     conn.reconnectAttempts >= conn.opts.maxReconnects:
    return false
  if conn.haveAttempt:
    let elapsed = (getMonoTime() - conn.lastAttempt).inMilliseconds
    if elapsed < reconnectDelayMs(conn.opts, conn.rng).int64:
      return false
  conn.tryReconnect()

proc connectionAlive*(conn: Connection): bool =
  ## Live socket, or successfully re-established after an outage.
  conn.connected or conn.ensureConnected()

proc outageReason(conn: Connection): string =
  ## Human-readable state of a dead connection, for error messages.
  var why = "connection lost"
  if conn.lastDisconnect.len > 0: why.add(" (" & conn.lastDisconnect & ")")
  if conn.opts.maxReconnects > 0 and
     conn.reconnectAttempts >= conn.opts.maxReconnects:
    why.add("; giving up after " & $conn.reconnectAttempts &
            " reconnect attempts")
  elif conn.reconnectAttempts > 0:
    why.add("; " & $conn.reconnectAttempts & " reconnect attempt(s) so far")
  elif not conn.opts.reconnect:
    why.add("; reconnecting is disabled")
  why

proc awaitConnected*(conn: Connection, timeoutMs: int, what: string): bool =
  ## Wait up to `timeoutMs` for a live connection, attempting reconnects as the
  ## window allows. False when the budget ran out; raises when waiting is
  ## pointless (explicitly closed, reconnecting disabled, attempts exhausted).
  ##
  ## This is what makes `flush(8000)` *wait out* an outage instead of failing
  ## because the next attempt is not due for another few hundred ms.
  let deadline = getMonoTime() + initDuration(milliseconds = timeoutMs)
  while true:
    if conn.connected: return true
    if conn.closed: fail(what & ": connection is closed")
    if not conn.opts.reconnect or conn.opts.maxReconnects == 0:
      fail(what & ": " & conn.outageReason())
    if conn.opts.maxReconnects > 0 and
       conn.reconnectAttempts >= conn.opts.maxReconnects:
      fail(what & ": " & conn.outageReason())
    if conn.ensureConnected(): return true
    let rem = (deadline - getMonoTime()).inMilliseconds
    if rem <= 0: return false
    # not yet due (the window spaces attempts): wait a slice and re-check
    sleep(min(rem, 25).int)

proc dial*(url: string, opts = defaultDialOptions()): Connection =
  ## Connect, complete the INFO/CONNECT handshake and validate it with a
  ## PING/PONG round trip (which is also when an auth failure surfaces).
  result = Connection(
    parser: initParser(),
    subs: initTable[int64, Subscription](),
    opts: opts,
    parts: parseUrl(url),
    pongsIn: 0,
    pingsOut: 0,
    serverErrors: @[])
  result.rng = initRand(int64(epochTime() * 1_000_000.0) +
                        int64(getCurrentProcessId()))
  result.setSink()
  result.sock = connectSocket(result.parts, opts.connectTimeoutMs)
  result.handshake()
  result.connected = true

proc close*(conn: Connection) =
  ## Explicit close: no reconnecting afterwards, buffered publishes dropped.
  if conn.closed and conn.sock == nil: return
  conn.closed = true
  conn.connected = false
  conn.pending.setLen(0)
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
  # While disconnected only the local record is kept: `resubscribe` sends every
  # live subscription (with its sid) after the reconnect, so a subscribe during
  # an outage takes effect then. No frame is buffered, which also means a
  # reconnect cannot double-register a sid.
  if conn.connected:
    var cmd = "SUB " & subject
    if queue.len > 0: cmd.add(" " & queue)
    cmd.add(" " & $result.sid & "\r\n")
    conn.writeDirect(cmd)

proc unsubscribe*(sub: Subscription, maxMsgs = 0) =
  ## Send UNSUB, drop the local queue and detach from the connection.
  if sub.closed: return
  sub.closed = true
  sub.msgs.clear()
  if sub.conn != nil:
    sub.conn.subs.del(sub.sid)
    # Same reasoning as subscribe: removing it locally is enough — a
    # reconnect resubscribes exactly what is still registered. With a live
    # socket the UNSUB goes out now, best effort (a dying connection is not
    # worth failing an unsubscribe over).
    if sub.conn.connected:
      var cmd = "UNSUB " & $sub.sid
      if maxMsgs > 0: cmd.add(" " & $maxMsgs)
      cmd.add("\r\n")
      try: sub.conn.writeDirect(cmd)
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
  let frame = h & data & "\r\n"
  if conn.connected:
    conn.writeDirect(frame)
    return
  # Disconnected: buffer, as Go does, and never drop silently. Past the cap the
  # publish fails — the caller decides, we do not pretend it was sent.
  if not conn.opts.reconnect:
    fail("publish to '" & subject & "': connection lost (" &
         conn.lastDisconnect & ") and reconnecting is disabled")
  if conn.opts.reconnectBufSize <= 0:
    fail("publish to '" & subject & "': connection lost (" &
         conn.lastDisconnect & ") and buffering is disabled")
  if conn.pending.len + frame.len > conn.opts.reconnectBufSize:
    fail("publish to '" & subject & "': reconnect buffer exceeded (" &
         $conn.pending.len & " + " & $frame.len & " > " &
         $conn.opts.reconnectBufSize & " bytes); " &
         "connection lost (" & conn.lastDisconnect & ")")
  conn.pending.add(frame)

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
    if conn == nil: fail("subscription has no connection")
    if conn.closed: fail("connection is closed")
    if not conn.connected:
      if conn.ensureConnected():
        continue
      # Disconnected and no attempt was due (or it failed). Consume this
      # call's budget first: the SDK pump polls with a 1 ms timeout and must
      # not spin at 100% CPU while the bus is down. Then report it — a lost
      # connection is an error, not a timeout.
      if timed:
        let rem = (deadline - getMonoTime()).inMilliseconds
        if rem > 0: sleep(rem.int)
      fail("no message on '" & sub.subject & "': " & conn.outageReason())
    var waitMs = 0
    if timed:
      let rem = (deadline - getMonoTime()).inMilliseconds
      if rem <= 0: break
      waitMs = rem.int
    try:
      discard conn.pump(waitMs)
    except NatsError:
      # The socket died while we were waiting. `noteDisconnect` has already
      # run, so the next turn of the loop takes the reconnect path; rethrow
      # only if the connection is somehow still considered alive.
      if conn.connected: raise
    if sub.msgs.len > 0: return sub.msgs.popFirst()
    if not conn.connected: continue
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
  ## subscriptions, never lost. A waiting call, so it drives a reconnect.
  let deadline = getMonoTime() + initDuration(milliseconds = timeoutMs)
  if not conn.connectionAlive():
    let rem = (deadline - getMonoTime()).inMilliseconds
    if rem <= 0 or not conn.awaitConnected(rem.int, "flush"):
      fail("flush: " & conn.outageReason())
  conn.writeDirect("PING\r\n")
  inc conn.pingsOut
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
  if not conn.connectionAlive() and
     not conn.awaitConnected(timeoutMs, "request"):
    fail("request: " & conn.outageReason())
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
