## Synchronous core NATS transport. No asyncdispatch or internal threads.
##
## Protocol behavior derives from nats.go @ 1ffb90b (Apache-2.0). Transport is
## a component-local nonblocking state machine, driven only by public calls.
## One connection belongs to one thread. Close connections explicitly.
## Reconnect progresses across even zero/one-ms polls without a blocking dial.
##
## Writes: a publish is on the wire (within its write budget) when it returns
## — there is no flusher thread to defer it, and callers must not need a
## follow-up operation to push their data. The reused output buffer avoids a
## fresh frame concatenation per publish; multi-frame operations (request's
## SUB+PUB, batched PONG replies) coalesce into one write internally, and
## publishes accepted during an outage are buffered for the reconnect.

import std/[deques, json, monotimes, nativesockets, net, os, random, strutils,
            tables, times, uri]
from std/posix import poll, TPollfd, Tnfds, POLLIN, POLLOUT, POLLERR, POLLHUP,
  POLLNVAL, EINTR, EAGAIN, EWOULDBLOCK, EINPROGRESS, Sockaddr_in, SockAddr,
  SockLen, connect
import natsnim/[parser, nuid, subject]

const
  defaultConnectTimeoutMs* = 5000
  defaultHandshakeTimeoutMs* = 10000
  defaultFlushTimeoutMs* = 5000
  defaultPendingLimit* = 65536
  defaultPendingBytes* = 16 * 1024 * 1024
  defaultConnectionPendingBytes* = 64 * 1024 * 1024
  readChunk = 65536
  maxOptionMs = 86_400_000
  errorLimit = 32
  directWriteMin = 16 * 1024
    ## Payloads at least this large bypass the buffer: one kernel copy beats
    ## buffer-copy plus kernel-copy for big frames.
  crlf = "\r\n"

type
  NatsError* = object of CatchableError
  NatsTimeout* = object of NatsError
  NoRespondersError* = object of NatsError
  ProtocolError = object of NatsError

  UrlParts* = object
    host*: string
    port*: int
    user*: string
    pass*: string

  DialOptions* = object
    connectTimeoutMs*, handshakeTimeoutMs*, writeTimeoutMs*: int
    clientName*: string
    reconnectWaitMs*, maxReconnects*, reconnectBufSize*: int
    reconnectDialTimeoutMs*, reconnectJitterMs*: int
    reconnect*: bool
    maxControlLine*, maxInboundPayload*, maxPendingBytes*: int

  ServerInfo* = object
    serverId*, serverName*, version*: string
    maxPayload*: int
    headers*, authRequired*: bool
    proto*: int
    raw*: JsonNode

  Message* = ref object
    subject*, reply*, data*, headers*: string
    sid*: int64
    size*, status*: int
    accountedBytes: int

  Subscription* = ref object
    owner: Connection
    subjectValue, queueValue: string
    sidValue: int64
    msgs: Deque[Message]
    pendingLimitValue, pendingByteLimit, bytes: int
    droppedValue: int64
    overrunValue, closedValue: bool
    received, autoMax, serverBaseReceived: int64

  Phase = enum offline, connecting, greeting, authenticating, validating, replaying, ready

  Connection* = ref object
    sock: Socket
    readBuf: string
    parser: Parser
    sink: Sink            ## parser callbacks, built once per connection
    subs: Table[int64, Subscription]
    infoValue: ServerInfo
    maxPayloadValue: int
    sidSeq: int64
    closedValue: bool
    phase: Phase
    opts: DialOptions
    parts: UrlParts
    address: Sockaddr_in  # DNS resolved once, never inside reconnect/poll calls
    phaseDeadline, nextAttemptAt: MonoTime
    attempts, reconnects: int
    initialAttempt: bool
    disconnectReason: string
    pending: string         ## accepted-but-unsent publishes (reconnect buffer)
    pendingSize, queuedBytes: int
    outgoing: string        ## staged handshake/replay frames
    outgoingOffset: int
    pendingStaged: int      ## bytes of `pending` staged into the current replay
    outBuf: string          ## output buffer: publishes awaiting one write
    outOffset: int
    rng: Rand
    ids: NuID
    errors: Deque[string]
    pingsOut, pongsIn: int64
    pendingPongs: int

proc defaultDialOptions*(): DialOptions =
  DialOptions(connectTimeoutMs: defaultConnectTimeoutMs,
    handshakeTimeoutMs: defaultHandshakeTimeoutMs, writeTimeoutMs: 5000,
    reconnectWaitMs: 2000, maxReconnects: 60, reconnectBufSize: 8 * 1024 * 1024,
    reconnectDialTimeoutMs: 2000, reconnectJitterMs: 100, reconnect: true,
    maxControlLine: MAX_CONTROL_LINE_SIZE, maxInboundPayload: defaultMaxInboundPayload,
    maxPendingBytes: defaultConnectionPendingBytes)

# Read-only views: callers cannot mutate the transport's state machine.
proc connected*(c: Connection): bool = c.phase == ready
proc closed*(c: Connection): bool = c.closedValue
proc info*(c: Connection): ServerInfo = c.infoValue
proc maxPayload*(c: Connection): int = c.maxPayloadValue
proc reconnectCount*(c: Connection): int = c.reconnects
proc reconnectAttempts*(c: Connection): int = c.attempts
proc lastDisconnect*(c: Connection): string = c.disconnectReason
proc subscriptionCount*(c: Connection): int = c.subs.len
proc pendingBytes*(c: Connection): int = c.queuedBytes
proc bufferedBytes*(c: Connection): int = c.pendingSize
proc unflushedBytes*(c: Connection): int =
  ## Publish bytes accepted but not yet written to the socket.
  c.outBuf.len - c.outOffset
proc subject*(s: Subscription): string = s.subjectValue
proc queue*(s: Subscription): string = s.queueValue
proc sid*(s: Subscription): int64 = s.sidValue
proc closed*(s: Subscription): bool = s.closedValue
proc dropped*(s: Subscription): int64 = s.droppedValue
proc overrun*(s: Subscription): bool = s.overrunValue
proc pendingCount*(s: Subscription): int = s.msgs.len
proc pendingBytes*(s: Subscription): int = s.bytes
proc pendingLimit*(s: Subscription): int = s.pendingLimitValue
proc serverErrors*(c: Connection): seq[string] =
  ## Snapshot of bounded, unconsumed server errors. Public waiting operations
  ## raise the oldest error; takeErrors is an explicit alternative drain.
  for e in c.errors: result.add(e)
proc takeErrors*(c: Connection): seq[string] =
  while c.errors.len > 0: result.add(c.errors.popFirst())

proc fail(msg: string) {.noreturn.} = raise newException(NatsError, msg)
proc timeout(what: string) {.noreturn.} = raise newException(NatsTimeout, what & ": timeout")
proc untilMs(ms: int): MonoTime =
  if ms < 0 or ms > maxOptionMs: fail("timeout must be between 0 and 86400000 ms")
  getMonoTime() + initDuration(milliseconds = ms)
proc leftMs(deadline: MonoTime): int =
  let ns = (deadline - getMonoTime()).inNanoseconds
  if ns <= 0: 0 else: int((ns + 999_999) div 1_000_000)
proc recordError(c: Connection, text: string) =
  if c.errors.len == errorLimit: discard c.errors.popFirst()
  c.errors.addLast(text)
proc checkErrors(c: Connection) =
  if c.errors.len > 0: fail("server: " & c.errors.popFirst())

proc dialTimeoutForAttempt*(opts: DialOptions): int =
  if opts.reconnectDialTimeoutMs > 0: opts.reconnectDialTimeoutMs
  else: opts.connectTimeoutMs
proc reconnectDelayMs*(opts: DialOptions, rng: var Rand): int =
  result = opts.reconnectWaitMs
  if opts.reconnectJitterMs > 0: result += rng.rand(opts.reconnectJitterMs - 1)
proc reconnectEligible(c: Connection): bool =
  not c.closedValue and c.opts.reconnect and c.opts.maxReconnects != 0 and
    (c.opts.maxReconnects < 0 or c.attempts < c.opts.maxReconnects)
proc reconnectDueInMs*(c: Connection): int =
  ## Read-only scheduler view; jitter is sampled once, not by this query.
  leftMs(c.nextAttemptAt)

proc dropSocket(c: Connection) =
  if c.sock != nil:
    c.sock.close()
    c.sock = nil
proc noteDisconnect(c: Connection, reason: string) =
  let wasReady = c.connected
  c.dropSocket()
  c.phase = offline
  c.disconnectReason = reason
  c.outgoing.setLen(0)
  c.outgoingOffset = 0
  if wasReady:
    c.attempts = 0
    # Publishes accepted before the outage but never written (the output
    # buffer) join the reconnect buffer: accepted means buffered, never
    # silently dropped. This may overshoot reconnectBufSize by at most one
    # buffered batch; the cap still applies to new publishes. outOffset
    # accounts for bytes a partial write already put on the wire.
    if c.outOffset < c.outBuf.len:
      c.pending.add(c.outBuf[c.outOffset .. ^1])
      c.pendingSize += c.outBuf.len - c.outOffset
  c.outBuf.setLen(0)
  c.outOffset = 0
  c.nextAttemptAt = getMonoTime() + initDuration(
    milliseconds = reconnectDelayMs(c.opts, c.rng))
proc outageReason(c: Connection): string =
  result = "connection lost (" & c.disconnectReason & ")"
  if not c.opts.reconnect or c.opts.maxReconnects == 0:
    result.add("; reconnecting is disabled")
  elif not c.reconnectEligible(): result.add("; reconnect attempts exhausted")

proc pollSocket(c: Connection, events: cshort, deadline: MonoTime): bool =
  ## EINTR consumes the same budget, never manufactures a disconnection.
  var fds: array[1, TPollfd]
  fds[0].fd = cint(c.sock.getFd())
  fds[0].events = events
  while true:
    let r = poll(addr fds[0], 1.Tnfds, leftMs(deadline).cint)
    if r > 0:
      return (fds[0].revents and (events or POLLERR or POLLHUP or POLLNVAL)) != 0
    if r == 0: return false
    let e = osLastError()
    if e.int == EINTR:
      if getMonoTime() >= deadline: return false
      continue
    c.noteDisconnect("poll: " & $e)
    fail("poll: " & $e)

proc sendWithin(c: Connection, data: string, offset: var int,
                deadline: MonoTime): bool =
  ## Exact offsets, no std/net string-send/SafeDisconn loop. False preserves
  ## progress for the reconnect state machine; ordinary writes disconnect on
  ## timeout, since a partial protocol frame cannot be abandoned on a live fd.
  while offset < data.len:
    let n = c.sock.send(unsafeAddr data[offset], data.len - offset)
    if n > 0:
      offset += n
    elif n == 0:
      c.noteDisconnect("send made no progress")
      fail("send made no progress")
    else:
      let e = osLastError()
      if e.int == EINTR:
        if getMonoTime() >= deadline: return false
        continue
      if e.int != EAGAIN and e.int != EWOULDBLOCK:
        c.noteDisconnect("send: " & $e)
        fail("send: " & $e)
      if not c.pollSocket(POLLOUT, deadline): return false
    if offset < data.len and getMonoTime() >= deadline: return false
  true

proc sendBytes(c: Connection, data: string, deadline: MonoTime) =
  ## Direct write of one frame. Fatal on failure: a partially written frame
  ## cannot be abandoned on a live fd, so the connection is marked lost.
  if c.sock == nil: fail("connection has no socket")
  var offset = 0
  if not c.sendWithin(data, offset, deadline):
    c.noteDisconnect("write timeout")
    fail("write timeout")

proc flushOut(c: Connection, deadline: MonoTime) =
  ## Write the buffered publishes to the socket as one send. Fatal on
  ## failure; the unwritten remainder is salvaged into the reconnect buffer.
  if c.outOffset >= c.outBuf.len:
    c.outBuf.setLen(0)
    c.outOffset = 0
    return
  if not c.sendWithin(c.outBuf, c.outOffset, deadline):
    c.noteDisconnect("write timeout")
    fail("write timeout")   # transport loss; remainder salvaged to pending
  c.outBuf.setLen(0)
  c.outOffset = 0

proc sendNow(c: Connection, data: string, deadline: MonoTime) =
  ## Ordered write: everything buffered earlier goes out first, so SUB/UNSUB/
  ## PING/PONG frames never overtake publishes issued before them.
  c.flushOut(deadline)
  c.sendBytes(data, deadline)

proc releaseBytes(s: Subscription, count: int) =
  s.bytes -= count
  if s.owner != nil: s.owner.queuedBytes -= count
proc clearQueue(s: Subscription) =
  s.releaseBytes(s.bytes)
  s.msgs.clear()
proc detach(s: Subscription, discardQueued: bool) =
  s.closedValue = true
  if s.owner != nil: s.owner.subs.del(s.sid)
  if discardQueued:
    s.clearQueue()
    s.owner = nil

proc headerStatus(headers: string): int =
  let eol = headers.find("\r\n")
  if eol < 0: raise newException(ProtocolError, "invalid NATS header")
  let line = headers[0 ..< eol]
  if line == "NATS/1.0": return 0
  if not line.startsWith("NATS/1.0 "):
    raise newException(ProtocolError, "invalid NATS header version")
  let rest = line[9 .. ^1].splitWhitespace()
  if rest.len == 0 or rest[0].len != 3: raise newException(ProtocolError, "invalid status")
  for ch in rest[0]:
    if ch notin {'0'..'9'}: raise newException(ProtocolError, "invalid status")
  parseInt(rest[0])

proc deliver*(c: Connection, a: MsgArgs, payload: string) =
  ## Parser/test seam. Messages, including statuses, stay in wire order.
  let s = c.subs.getOrDefault(a.sid)
  if s == nil: return
  if a.hdr < -1 or a.hdr > payload.len:
    raise newException(ProtocolError, "invalid header length")
  inc s.received
  let bytes = payload.len + a.subject.len + a.reply.len + 128
  if (s.pendingLimitValue > 0 and s.msgs.len >= s.pendingLimitValue) or
      bytes > s.pendingByteLimit - s.bytes or bytes > c.opts.maxPendingBytes - c.queuedBytes:
    inc s.droppedValue
    s.overrunValue = true
  else:
    let m = Message(subject: a.subject, reply: a.reply, sid: a.sid,
                    size: payload.len, accountedBytes: bytes)
    if a.hdr >= 0:
      m.headers = payload[0 ..< a.hdr]
      m.data = payload[a.hdr .. ^1]
      m.status = headerStatus(m.headers)
    else: m.data = payload
    s.msgs.addLast(m)
    s.bytes += bytes
    c.queuedBytes += bytes
  if s.autoMax > 0 and s.received >= s.autoMax:
    s.detach(false) # queued messages may still be drained

proc applyInfo(c: Connection, raw: string) =
  let j = parseJson(raw)
  if j.kind != JObject: raise newException(ProtocolError, "INFO must be an object")
  c.infoValue = ServerInfo(raw: j, serverId: j{"server_id"}.getStr(),
    serverName: j{"server_name"}.getStr(), version: j{"version"}.getStr(),
    maxPayload: j{"max_payload"}.getInt(), headers: j{"headers"}.getBool(),
    authRequired: j{"auth_required"}.getBool(), proto: j{"proto"}.getInt())
  if c.infoValue.maxPayload <= 0: raise newException(ProtocolError, "invalid max_payload")
  c.maxPayloadValue = c.infoValue.maxPayload

proc makeSink(c: Connection): Sink =
  ## Parser callbacks closed over the connection. Built once in dial; the
  ## cycle (connection -> sink -> connection) is broken in close().
  proc onMsg(a: MsgArgs, p: string) = c.deliver(a, p)
  proc onPing() = inc c.pendingPongs
  proc onPong() = inc c.pongsIn
  proc onInfo(raw: string) = c.applyInfo(raw)
  proc onErr(text: string) = c.recordError(text)
  Sink(onMsg: onMsg, onPing: onPing, onPong: onPong,
       onInfo: onInfo, onErr: onErr)

proc readSome(c: Connection, deadline: MonoTime): bool =
  if not c.pollSocket(POLLIN, deadline): return false
  c.readBuf.setLen(readChunk)
  let n = c.sock.recv(addr c.readBuf[0], readChunk)
  if n < 0:
    let e = osLastError()
    if e.cint in [EINTR, EAGAIN, EWOULDBLOCK]: return false
    c.noteDisconnect("recv: " & $e)
    fail("recv: " & $e)
  if n == 0:
    c.noteDisconnect("connection closed by server")
    fail("connection closed by server")
  c.readBuf.setLen(n)
  try:
    # The general parser accepts arbitrary caller callbacks. This invocation
    # uses only the connection-owned sink closures (built once in dial).
    {.cast(gcsafe).}:
      let err = c.parser.parse(c.readBuf, c.sink)
      if err.len > 0: raise newException(ProtocolError, err)
  except CatchableError as e:
    c.noteDisconnect("protocol: " & e.msg)
    raise newException(ProtocolError, e.msg)
  # Reads only happen after any staged write is finished; never interleave a
  # PONG into the middle of a partially sent CONNECT/SUB/PUB frame.
  if c.pendingPongs > 0:
    let pongs = repeat("PONG\r\n", c.pendingPongs)
    c.pendingPongs = 0
    c.sendNow(pongs, deadline)
  if c.errors.len > 0:
    let text = c.errors.popFirst()
    if not c.connected or not text.contains("Permissions Violation"):
      c.noteDisconnect("server: " & text)
    fail("server: " & text)
  true

proc parseUrl*(url: string): UrlParts =
  ## IPv4 TCP only. Hostname resolution is a synchronous setup step in dial;
  ## use numeric addresses when even resolver latency must be avoided.
  var s = url
  if s.contains("://"):
    if not (s.startsWith("nats://") or s.startsWith("tcp://")):
      fail("unsupported server URL scheme (expected nats or tcp)")
    s = s[s.find("://") + 3 .. ^1]
  if s.len == 0 or s.contains('/') or s.contains('?') or s.contains('#'):
    fail("invalid server URL")
  let at = s.rfind('@')
  if at >= 0:
    let cred = s[0 ..< at]
    s = s[at + 1 .. ^1]
    let colon = cred.find(':')
    if colon < 0: fail("URL credentials require user:password; tokens are unsupported")
    result.user = decodeUrl(cred[0 ..< colon], decodePlus = false)
    result.pass = decodeUrl(cred[colon + 1 .. ^1], decodePlus = false)
  if s.count(':') > 1 or s.contains('[') or s.contains(']'):
    fail("IPv6 URLs are not supported")
  let colon = s.find(':')
  result.host = if colon < 0: s else: s[0 ..< colon]
  result.port = 4222
  if colon >= 0:
    try: result.port = parseInt(s[colon + 1 .. ^1])
    except ValueError: fail("invalid server port")
  if result.host.len == 0 or result.port <= 0 or result.port > 65535:
    fail("invalid server address or port")
  for ch in result.host:
    if ch in {'\0'..' ', '@'}: fail("invalid server hostname")

proc validateOptions(o: DialOptions) =
  for t in [o.connectTimeoutMs, o.handshakeTimeoutMs, o.writeTimeoutMs]:
    if t <= 0 or t > maxOptionMs: fail("invalid connection timeout option")
  for t in [o.reconnectWaitMs, o.reconnectDialTimeoutMs, o.reconnectJitterMs]:
    if t < 0 or t > maxOptionMs div 2: fail("invalid reconnect timing option")
  if o.maxReconnects < -1 or o.reconnectBufSize < 0 or o.maxPendingBytes <= 0:
    fail("invalid connection limit option")
  discard initParser(o.maxControlLine, o.maxInboundPayload)

proc startAttempt(c: Connection) =
  if not c.initialAttempt: inc c.attempts
  c.parser = initParser(c.opts.maxControlLine, c.opts.maxInboundPayload)
  c.infoValue = ServerInfo()
  c.pingsOut = 0
  c.pongsIn = 0
  c.pendingPongs = 0
  c.sock = newSocket(buffered = false)
  setBlocking(c.sock.getFd(), false)
  c.sock.setSockOpt(OptNoDelay, true, level = IPPROTO_TCP.cint)
  c.phase = connecting
  c.phaseDeadline = untilMs(if c.initialAttempt: c.opts.connectTimeoutMs
                            else: dialTimeoutForAttempt(c.opts))
  let rc = connect(c.sock.getFd(), cast[ptr SockAddr](addr c.address),
                   sizeof(c.address).SockLen)
  if rc < 0:
    let e = osLastError()
    if e.cint notin [EINPROGRESS, EINTR, EWOULDBLOCK]:
      fail("connect: " & $e)

proc stage(c: Connection, text: string) =
  c.outgoing = text
  c.outgoingOffset = 0
proc stageReplay(c: Connection) =
  var text = ""
  for sid, sub in c.subs:
    sub.serverBaseReceived = sub.received
    text.add("SUB " & sub.subject)
    if sub.queue.len > 0: text.add(" " & sub.queue)
    text.add(" " & $sid & "\r\n")
    if sub.autoMax > 0:
      text.add("UNSUB " & $sid & " " & $(sub.autoMax - sub.received) & "\r\n")
  if c.pending.len > 0:
    # Flat wire-format blob. Oversize was checked at publish time against the
    # then-current max_payload; the server re-enforces its own limit and any
    # -ERR surfaces through the normal error path.
    text.add(c.pending)
    c.pendingStaged = c.pending.len
  c.stage(text)
  c.phase = replaying

proc advanceAttempt(c: Connection, deadline: MonoTime): bool =
  ## Bounded progress, retained across calls. No reader/worker thread required.
  for step in 0 ..< 64:
    if c.phase == offline:
      if not c.reconnectEligible() or getMonoTime() < c.nextAttemptAt: return false
      c.startAttempt()
    let budget = min(deadline, c.phaseDeadline)
    if getMonoTime() >= c.phaseDeadline: timeout("connect/handshake")
    case c.phase
    of connecting:
      if not c.pollSocket(POLLOUT, budget): return false
      let err = getSockOptInt(c.sock.getFd(), SOL_SOCKET, SO_ERROR)
      if err != 0: fail("connect: " & $err)
      c.phase = greeting
      c.phaseDeadline = untilMs(c.opts.handshakeTimeoutMs)
    of greeting:
      if c.infoValue.raw == nil:
        if not c.readSome(budget): return false
      if c.infoValue.raw != nil:
        var j = %*{"verbose": false, "pedantic": false, "lang": "nim",
          "version": "0.1.0", "protocol": 1, "echo": true,
          "headers": c.infoValue.headers, "no_responders": c.infoValue.headers}
        if c.opts.clientName.len > 0: j["name"] = %c.opts.clientName
        if c.parts.user.len > 0:
          j["user"] = %c.parts.user
          j["pass"] = %c.parts.pass
        c.stage("CONNECT " & $j & "\r\nPING\r\n")
        inc c.pingsOut
        c.phase = authenticating
    of authenticating, replaying:
      if not c.sendWithin(c.outgoing, c.outgoingOffset, budget): return false
      c.outgoing.setLen(0)
      c.outgoingOffset = 0
      if c.phase == authenticating:
        c.phase = validating
      else:
        # The staged snapshot is on the wire: retire exactly those bytes and
        # stage publishes that arrived meanwhile as a follow-up batch.
        if c.pendingStaged > 0:
          c.pendingSize -= c.pendingStaged
          c.pending = c.pending[c.pendingStaged .. ^1]
          c.pendingStaged = 0
        if c.pending.len > 0:
          var text = ""
          text.add(c.pending)
          c.pendingStaged = c.pending.len
          c.stage(text)
        else:
          c.phase = ready
          c.attempts = 0
          if not c.initialAttempt: inc c.reconnects
          c.initialAttempt = false
          return true
    of validating:
      if c.pongsIn < c.pingsOut:
        if not c.readSome(budget): return false
      if c.pongsIn >= c.pingsOut: c.stageReplay()
    of ready: return true
    of offline: return false
    if getMonoTime() >= deadline: return false
  false

proc ensureConnected(c: Connection, deadline: MonoTime): bool =
  if c.closed: fail("connection is closed")
  if c.connected: return true
  try:
    c.advanceAttempt(deadline)
  except CatchableError as e:
    if c.phase != offline: c.noteDisconnect(e.msg)
    if c.initialAttempt: raise
    false

proc awaitUntil(c: Connection, deadline: MonoTime): bool =
  while true:
    c.checkErrors()
    if c.ensureConnected(deadline): return true
    if c.phase == offline and not c.reconnectEligible(): fail(c.outageReason())
    if getMonoTime() >= deadline: return false
    if c.phase == offline:
      sleep(min(leftMs(deadline), max(1, min(25, c.reconnectDueInMs))))

proc connectionAlive*(c: Connection): bool =
  ## Nonblocking readiness/progress probe; may begin but never wait for a dial.
  c.ensureConnected(getMonoTime())
proc awaitConnected*(c: Connection, timeoutMs: int, what: string): bool =
  discard what
  c.awaitUntil(untilMs(timeoutMs))

proc close*(c: Connection) =
  if c == nil: return
  c.closedValue = true
  c.dropSocket()
  c.phase = offline
  # Break every connection/subscription cycle, including under ARC.
  for s in c.subs.values:
    s.closedValue = true
    s.clearQueue()
    s.owner = nil
  c.subs.clear()
  c.pending.setLen(0)
  c.pendingSize = 0
  c.outgoing.setLen(0)
  c.outBuf.setLen(0)
  c.outOffset = 0
  c.sink = Sink()       # break connection -> sink -> connection
  c.readBuf.setLen(0)

proc dial*(url: string, opts = defaultDialOptions()): Connection =
  ## Resolve the IPv4 address once, then connect and handshake with bounded
  ## transport phases. OS hostname resolution is synchronous and outside these
  ## phase budgets; numeric IPs avoid DNS. Reconnect never resolves names.
  validateOptions(opts)
  let parts = parseUrl(url)
  result = Connection(parts: parts, opts: opts, phase: offline,
    initialAttempt: true, subs: initTable[int64, Subscription](),
    errors: initDeque[string](), ids: newNuID(),
    rng: initRand(int64(epochTime() * 1_000_000) + getCurrentProcessId().int64))
  result.sink = makeSink(result)
  try:
    let ai = getAddrInfo(parts.host, Port(parts.port), AF_INET)
    defer: freeAddrInfo(ai)
    copyMem(addr result.address, ai.ai_addr, sizeof(result.address))
    result.startAttempt()
    let deadline = getMonoTime() + initDuration(
      milliseconds = opts.connectTimeoutMs.int64 + opts.handshakeTimeoutMs)
    if not result.awaitUntil(deadline): timeout("dial")
  except:
    result.close()
    raise

proc pump*(c: Connection, timeoutMs: int): bool =
  ## Read one available chunk (also advances reconnect). Errors are surfaced.
  let deadline = untilMs(timeoutMs)
  c.checkErrors()
  if not c.awaitUntil(deadline): return false
  c.readSome(deadline)

proc subscribe*(c: Connection, subject: string, queue = "",
                pendingLimit = defaultPendingLimit,
                pendingBytes = defaultPendingBytes): Subscription =
  if c.closed: fail("connection is closed")
  if badSubject(subject) or subject.len > c.opts.maxControlLine - 64:
    fail("invalid or oversized subject")
  if badQueue(queue) or subject.len + queue.len > c.opts.maxControlLine - 64:
    fail("invalid or oversized queue")
  if pendingLimit < 0 or pendingBytes <= 0: fail("invalid pending limits")
  if c.sidSeq == high(int64): fail("subscription ids exhausted")
  inc c.sidSeq
  result = Subscription(owner: c, subjectValue: subject, queueValue: queue,
    sidValue: c.sidSeq, msgs: initDeque[Message](),
    pendingLimitValue: pendingLimit, pendingByteLimit: pendingBytes)
  c.subs[result.sid] = result
  if c.phase == replaying: c.noteDisconnect("subscriptions changed during replay")
  if c.connected:
    var cmd = "SUB " & subject
    if queue.len > 0: cmd.add(" " & queue)
    cmd.add(" " & $result.sid & "\r\n")
    try: c.sendNow(cmd, untilMs(c.opts.writeTimeoutMs))
    except:
      result.detach(true)
      raise

proc unsubscribe*(s: Subscription, maxMsgs = 0) =
  ## Zero: detach immediately and discard queued messages. Positive: stop
  ## after this total received count, preserving queued messages for draining.
  if maxMsgs < 0: fail("negative auto-unsubscribe limit")
  let c = s.owner
  if maxMsgs > 0 and not s.closed:
    s.autoMax = maxMsgs
    if s.received < s.autoMax:
      if c.phase == replaying: c.noteDisconnect("subscriptions changed during replay")
      if c.connected:
        try: c.sendNow("UNSUB " & $s.sid & " " & $(maxMsgs.int64 - s.serverBaseReceived) & "\r\n", untilMs(c.opts.writeTimeoutMs))
        except CatchableError: discard
      return
  if c != nil:
    if c.phase == replaying: c.noteDisconnect("subscriptions changed during replay")
    if not s.closed and c.connected:
      try: c.sendNow("UNSUB " & $s.sid & "\r\n", untilMs(c.opts.writeTimeoutMs))
      except CatchableError: discard
  s.detach(maxMsgs == 0)

proc stagePublish(c: Connection, h, data: string, deadline: MonoTime) =
  ## Append one publish frame to the output buffer without flushing — or, for
  ## large payloads on an empty buffer, write it straight through (one kernel
  ## copy instead of buffer-copy plus kernel-copy). Callers that coalesce
  ## several frames (request: SUB + PUB) flush explicitly.
  if c.outBuf.len == 0 and data.len >= directWriteMin:
    c.sendBytes(h, deadline)
    c.sendBytes(data, deadline)
    c.sendBytes(crlf, deadline)
  else:
    c.outBuf.add(h)
    c.outBuf.add(data)
    c.outBuf.add(crlf)

proc publishRaw(c: Connection, subject, reply, data: string, deadline: MonoTime) =
  if c.closed: fail("connection is closed")
  if badSubject(subject) or subject.contains('*') or subject.contains('>') or
      (reply.len > 0 and (badSubject(reply) or reply.contains('*') or reply.contains('>'))):
    fail("invalid publish/reply subject")
  if subject.len + reply.len > c.opts.maxControlLine - 64: fail("publish subject too long")
  if c.maxPayload > 0 and data.len > c.maxPayload:
    fail("payload exceeds server max_payload")
  let h = "PUB " & subject & (if reply.len > 0: " " & reply else: "") & " " & $data.len & "\r\n"
  if c.connected:
    # Go batches publishes behind a flusher goroutine; this design has no
    # thread, so the wire-write happens here: a publish is on the wire
    # (within the write budget) when it returns. Callers never need a
    # follow-up operation to flush it, which also keeps cross-connection
    # request/reply patterns deadlock-free. The reused output buffer avoids
    # a fresh concatenation per publish; large payloads skip even its copy.
    c.stagePublish(h, data, deadline)
    c.flushOut(deadline)
    return
  if c.phase == offline and not c.reconnectEligible(): fail("publish: " & c.outageReason())
  let frameLen = h.len + data.len + 2
  if frameLen > c.opts.reconnectBufSize - c.pendingSize:
    fail("reconnect buffer exceeded")
  c.pending.add(h)
  c.pending.add(data)
  c.pending.add(crlf)
  c.pendingSize += frameLen

proc publish*(c: Connection, subject, data: string) =
  c.publishRaw(subject, "", data, untilMs(c.opts.writeTimeoutMs))
proc publishRequest*(c: Connection, subject, reply, data: string) =
  c.publishRaw(subject, reply, data, untilMs(c.opts.writeTimeoutMs))

proc pop(s: Subscription): Message =
  if s.overrunValue:
    s.overrunValue = false # report once, then allow recovery/draining
    fail("subscription exceeded its pending limit; " & $s.dropped & " message(s) dropped")
  if s.msgs.len == 0: return nil
  result = s.msgs.popFirst()
  s.releaseBytes(result.accountedBytes)
  if s.closed and s.msgs.len == 0: s.owner = nil
  if result.status == 503 and result.data.len == 0:
    raise newException(NoRespondersError, "no responders available for '" & s.subject & "'")

proc take*(s: Subscription): Message =
  ## Pop queued data without reading; error/status handling matches nextMsg.
  s.pop()

proc nextUntil(s: Subscription, deadline: MonoTime): Message =
  while true:
    result = s.pop()
    if result != nil: return
    if s.closed: fail("subscription is closed")
    let c = s.owner
    if c == nil or c.closed: fail("connection is closed")
    c.checkErrors()
    if not c.awaitUntil(deadline): timeout("reconnect")
    try:
      # Deliver our own buffered writes before blocking on the socket: a
      # publish enqueued by this caller must reach the server while we wait.
      c.flushOut(deadline)
      discard c.readSome(deadline)
    except ProtocolError: raise
    except NatsTimeout: raise
    except NatsError:
      if c.connected: raise # permissions error, not a transport failure
      if not c.reconnectEligible(): raise
      if getMonoTime() >= deadline: raise
      continue
    # Always inspect the result of the last read, including zero-ms polls.
    result = s.pop()
    if result != nil: return
    if getMonoTime() >= deadline: timeout("nextMsg")

proc nextMsg*(s: Subscription, timeoutMs: int): Message =
  ## Read/poll with one absolute budget, including reconnect and PONG writes.
  ## Zero never waits, but can advance a nonblocking reconnect step.
  s.nextUntil(untilMs(timeoutMs))
proc tryNextMsg*(s: Subscription, timeoutMs: int): Message =
  try: s.nextMsg(timeoutMs)
  except NatsTimeout: nil

proc flush*(c: Connection, timeoutMs = defaultFlushTimeoutMs) =
  ## PING/PONG barrier, not a publish acknowledgement. Server errors surface.
  let deadline = untilMs(timeoutMs)
  c.checkErrors()
  if not c.awaitUntil(deadline): timeout("flush reconnect")
  # Buffered publishes precede the barrier, or the PONG could outrun them.
  c.flushOut(deadline)
  c.sendBytes("PING\r\n", deadline)
  inc c.pingsOut
  while c.pongsIn < c.pingsOut:
    if getMonoTime() >= deadline: timeout("flush")
    discard c.readSome(deadline)

proc newInbox*(c: Connection): string = "_INBOX." & c.ids.next()
proc request*(c: Connection, subject, data: string,
              timeoutMs = defaultFlushTimeoutMs): Message =
  ## A single budget covers reconnect, subscription, publish, receive and
  ## cleanup. Do not replay a timed-out request later from an outage buffer.
  let deadline = untilMs(timeoutMs)
  c.checkErrors()
  if not c.awaitUntil(deadline): timeout("request reconnect")
  # Register locally; SUB, PUB and cleanup all share one budget.
  let inbox = c.newInbox()
  if c.sidSeq == high(int64): fail("subscription ids exhausted")
  inc c.sidSeq
  let s = Subscription(owner: c, subjectValue: inbox, sidValue: c.sidSeq,
    msgs: initDeque[Message](), pendingLimitValue: defaultPendingLimit,
    pendingByteLimit: defaultPendingBytes)
  c.subs[s.sid] = s
  try:
    if data.len >= directWriteMin:
      # Large payload: write the SUB out, then let the publish bypass the
      # buffer entirely (wire order is SUB before PUB either way).
      c.sendNow("SUB " & inbox & " " & $s.sid & "\r\n", deadline)
      c.publishRaw(subject, inbox, data, deadline)
    else:
      # Coalesce SUB + PUB into one write: publishRaw's flush sends both.
      c.outBuf.add("SUB " & inbox & " " & $s.sid & "\r\n")
      c.publishRaw(subject, inbox, data, deadline)
    result = s.nextUntil(deadline)
  finally:
    if c.connected:
      try: c.sendNow("UNSUB " & $s.sid & "\r\n", deadline)
      except CatchableError: discard
    s.detach(true)
