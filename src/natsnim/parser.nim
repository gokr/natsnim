## Protocol parser: the byte-level state machine for the NATS wire protocol.
##
## Derived from `nats-io/nats.go` `parser.go` @ 1ffb90b (Apache-2.0).
## See PROVENANCE.md.
##
## Deliberate deviations from the Go original, all having the same observable
## behaviour:
##
##  - **No buffer reuse.** Go keeps a 4 KiB `scratch` array and points
##    `argBuf`/`msgBuf` into it (and into the read buffer) to avoid
##    allocations, with `cloneMsgArg` to escape the read buffer's lifetime.
##    Here split arguments and split payloads accumulate in Nim strings, and
##    `MsgArgs` fields are always owned copies — so `cloneMsgArg` has no
##    counterpart. Explicit control-line and payload limits reject oversized
##    frames before buffering them.
##  - **The payload is always copied** into a fresh string, where Go may hand
##    the handler a slice of the read buffer. A message that spans reads is
##    therefore assembled in `msgBuf` in both implementations.
##  - **No statistics.** Go's `Conn.Statistics` (InMsgs/InBytes) is updated in
##    `processMsg`; here msgs/bytes accounting belongs to the connection, i.e.
##    the sink. Tests count in the sink.
##  - The state enum is kept in Go's exact iota order, so a parse error's
##    numeric state matches upstream (`fail` prints it).


const
  MAX_CONTROL_LINE_SIZE* = 4096
  defaultMaxInboundPayload* = 16 * 1024 * 1024

type
  PState* = enum
    ## Same order (and therefore the same numeric values) as nats.go's iota.
    OP_START, OP_PLUS, OP_PLUS_O, OP_PLUS_OK, OP_MINUS, OP_MINUS_E,
    OP_MINUS_ER, OP_MINUS_ERR, OP_MINUS_ERR_SPC, MINUS_ERR_ARG,
    OP_M, OP_MS, OP_MSG, OP_MSG_SPC, MSG_ARG, MSG_PAYLOAD, MSG_END,
    OP_H, OP_P, OP_PI, OP_PIN, OP_PING, OP_PO, OP_PON, OP_PONG,
    OP_I, OP_IN, OP_INF, OP_INFO, OP_INFO_SPC, INFO_ARG

  MsgArgs* = object
    ## Parsed `MSG`/`HMSG` argument line.
    subject*: string
    reply*: string
    sid*: int64
    hdr*: int      ## header length for HMSG, -1 for MSG
    size*: int     ## total payload size (headers included for HMSG)

  Sink* = object
    ## Callbacks invoked as complete operations are recognised. Any field may
    ## be nil (that operation is then ignored).
    onMsg*: proc(a: MsgArgs, payload: string)
    onOK*: proc()
    onErr*: proc(text: string)
    onPing*: proc()
    onPong*: proc()
    onInfo*: proc(json: string)

  Parser* = object
    state*: PState
    maxControlLine: int
    maxPayload: int
    controlBytes: int
    hdr: int
    argStart: int    ## start of the current argument
    drop: int      ## 1 when a '\r' must be dropped before the '\n'
    ma*: MsgArgs
    argBuf*: string     ## partial argument (control line); internal
    argActive: bool   ## Go's `argBuf != nil`
    msgBuf*: string     ## partial message payload; internal
    msgActive: bool   ## Go's `msgBuf != nil`

proc hasArgBuf*(p: Parser): bool = p.argActive
  ## The port of a Go test's `ps.argBuf != nil`.

proc hasMsgBuf*(p: Parser): bool = p.msgActive
  ## The port of a Go test's `ps.msgBuf != nil`.

proc initParser*(maxControlLine = MAX_CONTROL_LINE_SIZE,
                 maxPayload = defaultMaxInboundPayload): Parser =
  if maxControlLine <= 0 or maxPayload <= 0 or maxPayload > high(int) div 2:
    raise newException(ValueError, "invalid parser limits")
  Parser(state: OP_START, hdr: 0, ma: MsgArgs(hdr: 0),
         maxControlLine: maxControlLine, maxPayload: maxPayload)

# --- argument parsing -------------------------------------------------------

proc sameBytes(s: string, d: string, start, stop: int): bool =
  ## s equals d[start ..< stop]; comparison without allocation.
  if s.len != stop - start: return false
  for i in 0 ..< s.len:
    if s[i] != d[start + i]: return false
  true

proc setToken(field: var string, d: string, start, stop: int) =
  ## Own a token's bytes, reusing the existing allocation when they match.
  ## Subjects repeat heavily on a bus, so the common case is a byte compare
  ## instead of a fresh string. A reused string is shared by reference and
  ## never mutated: replacing it allocates a new one.
  if not sameBytes(field, d, start, stop):
    field = d[start ..< stop]

proc nextToken(d: string, fromPos, stop: int): tuple[found: bool, s, e: int] =
  ## Bounds of the next whitespace-delimited token within [fromPos, stop).
  ## The stop bound is what keeps the trailing-token check from wandering
  ## into the payload that follows the argument line.
  var i = fromPos
  while i < stop and d[i] in {' ', '\t', '\r', '\n'}: inc i
  if i >= stop: return
  result.s = i
  while i < stop and d[i] notin {' ', '\t', '\r', '\n'}: inc i
  result.e = i
  result.found = true

proc parseInt64Range(d: string, start, stop: int): int64 =
  ## Decimal digits in [start, stop); -1 signals an error (as upstream).
  ## Range-based so the hot path never allocates a token string.
  if stop <= start: return -1
  var n = 0'i64
  for i in start ..< stop:
    let dec = d[i]
    if dec < '0' or dec > '9': return -1
    let digit = int64(ord(dec)) - 48
    if n > (high(int64) - digit) div 10: return -1
    n = n * 10 + digit
  n

proc parseInt64*(d: string): int64 =
  ## Decimal positive numbers; -1 signals an error (as upstream).
  parseInt64Range(d, 0, d.len)

proc boundedSizeRange(d: string, start, stop: int, limit: int): int =
  let n = parseInt64Range(d, start, stop)
  if n < 0 or n > limit: -1 else: n.int

proc processHeaderMsgArgs(p: var Parser, d: string, start, stop: int): string =
  ## `HMSG subject sid [reply] hdr size` — tokens are scanned in place;
  ## subject/reply strings are owned and reused when unchanged, numbers never
  ## allocate. Error text matches upstream's splitArgs-based messages.
  let quoted = d[start ..< stop]
  template perr: string = "nats: processHeaderMsgArgs Parse Error: '" & quoted & "'"
  var tok = nextToken(d, start, stop)
  if not tok.found: return perr
  setToken(p.ma.subject, d, tok.s, tok.e)
  tok = nextToken(d, tok.e, stop)
  if not tok.found: return perr
  p.ma.sid = parseInt64Range(d, tok.s, tok.e)
  let t3 = nextToken(d, tok.e, stop)
  if not t3.found: return perr
  let t4 = nextToken(d, t3.e, stop)
  if not t4.found: return perr
  let t5 = nextToken(d, t4.e, stop)
  if t5.found:
    setToken(p.ma.reply, d, t3.s, t3.e)
    p.ma.hdr = boundedSizeRange(d, t4.s, t4.e, p.maxPayload)
    p.ma.size = boundedSizeRange(d, t5.s, t5.e, p.maxPayload)
    if nextToken(d, t5.e, stop).found: return perr
  else:
    p.ma.reply = ""
    p.ma.hdr = boundedSizeRange(d, t3.s, t3.e, p.maxPayload)
    p.ma.size = boundedSizeRange(d, t4.s, t4.e, p.maxPayload)
  if p.ma.sid < 0:
    return "nats: processHeaderMsgArgs Bad or Missing Sid: '" & quoted & "'"
  if p.ma.hdr < 0 or p.ma.hdr > p.ma.size:
    return "nats: processHeaderMsgArgs Bad or Missing Header Size: '" & quoted & "'"
  if p.ma.size < 0:
    return "nats: processHeaderMsgArgs Bad or Missing Size: '" & quoted & "'"
  ""

proc processMsgArgsRange(p: var Parser, d: string, start, stop: int): string =
  ## Parse the `MSG`/`HMSG` argument line into `p.ma`; "" on success. Valid
  ## forms: `MSG subject sid size`, `MSG subject sid reply size`, and the
  ## HMSG forms with a header length; anything else is upstream's Parse
  ## Error. The parser's `hdr` field selects the HMSG variant.
  if p.hdr >= 0:
    return p.processHeaderMsgArgs(d, start, stop)
  let quoted = d[start ..< stop]
  template perr: string = "nats: processMsgArgs Parse Error: '" & quoted & "'"
  var tok = nextToken(d, start, stop)
  if not tok.found: return perr
  setToken(p.ma.subject, d, tok.s, tok.e)
  tok = nextToken(d, tok.e, stop)
  if not tok.found: return perr
  p.ma.sid = parseInt64Range(d, tok.s, tok.e)
  let t3 = nextToken(d, tok.e, stop)
  if not t3.found: return perr
  let t4 = nextToken(d, t3.e, stop)
  if t4.found:
    setToken(p.ma.reply, d, t3.s, t3.e)
    p.ma.size = boundedSizeRange(d, t4.s, t4.e, p.maxPayload)
    if nextToken(d, t4.e, stop).found: return perr
  else:
    p.ma.reply = ""
    p.ma.size = boundedSizeRange(d, t3.s, t3.e, p.maxPayload)
  if p.ma.sid < 0:
    return "nats: processMsgArgs Bad or Missing Sid: '" & quoted & "'"
  if p.ma.size < 0:
    return "nats: processMsgArgs Bad or Missing Size: '" & quoted & "'"
  ""

proc processMsgArgs*(p: var Parser, arg: string): string =
  ## Whole-string entry point (tests and external callers).
  p.processMsgArgsRange(arg, 0, arg.len)

# --- the state machine ------------------------------------------------------

proc fail(state: PState, buf: string, i: int): string =
  "nats: Parse Error [" & $ord(state) & "]: '" & buf[i .. ^1] & "'"

proc parse*(p: var Parser, buf: string, sink: Sink): string =
  ## Consume `buf`, invoking `sink` for every complete operation. Returns ""
  ## on success or a parse error message. Arbitrary chunk boundaries are
  ## supported (split args, split payloads) — that is what upstream's
  ## TestParserSplitMsg covers.
  var i = 0
  while i < buf.len:
    let b = buf[i]
    if p.state != MSG_PAYLOAD:
      inc p.controlBytes
      if p.controlBytes > p.maxControlLine:
        return "nats: control line exceeds limit"
      if b == '\n': p.controlBytes = 0
    case p.state
    of OP_START:
      case b
      of 'M', 'm':
        p.state = OP_M
        p.hdr = -1
        p.ma.hdr = -1
      of 'H', 'h':
        p.state = OP_H
        p.hdr = 0
        p.ma.hdr = 0
      of 'P', 'p': p.state = OP_P
      of '+': p.state = OP_PLUS
      of '-': p.state = OP_MINUS
      of 'I', 'i': p.state = OP_I
      else: return fail(p.state, buf, i)
    of OP_H:
      case b
      of 'M', 'm': p.state = OP_M
      else: return fail(p.state, buf, i)
    of OP_M:
      case b
      of 'S', 's': p.state = OP_MS
      else: return fail(p.state, buf, i)
    of OP_MS:
      case b
      of 'G', 'g': p.state = OP_MSG
      else: return fail(p.state, buf, i)
    of OP_MSG:
      case b
      of ' ', '\t': p.state = OP_MSG_SPC
      else: return fail(p.state, buf, i)
    of OP_MSG_SPC:
      case b
      of ' ', '\t':
        inc i
        continue
      else:
        p.state = MSG_ARG
        p.argStart = i
    of MSG_ARG:
      case b
      of '\r':
        p.drop = 1
      of '\n':
        # Scan the argument line in place: no arg-line string is allocated on
        # the hot path (split lines still use argBuf).
        let e = if p.argActive:
          p.processMsgArgsRange(p.argBuf, 0, p.argBuf.len)
        else:
          p.processMsgArgsRange(buf, p.argStart, i - p.drop)
        if e.len > 0: return e
        p.drop = 0
        p.argStart = i + 1
        p.state = MSG_PAYLOAD
        # Jump ahead: the payload follows immediately. If this overruns what
        # is left, the loop falls out and the post-loop handles the split.
        # Bound arithmetic by this buffer, not an attacker-controlled size.
        i = p.argStart + min(p.ma.size, buf.len - p.argStart) - 1
      else:
        if p.argActive: p.argBuf.add(b)
    of MSG_PAYLOAD:
      if p.msgActive:
        if p.msgBuf.len >= p.ma.size:
          if sink.onMsg != nil: sink.onMsg(p.ma, p.msgBuf)
          p.argActive = false
          p.argBuf.setLen(0)
          p.msgActive = false
          p.msgBuf.setLen(0)
          p.state = MSG_END
        else:
          # Copy as much as we can and skip ahead.
          var toCopy = p.ma.size - p.msgBuf.len
          let avail = buf.len - i
          if avail < toCopy: toCopy = avail
          if toCopy > 0:
            p.msgBuf.add(buf[i ..< i + toCopy])
            i = (i + toCopy) - 1
          else:
            p.msgBuf.add(b)
      elif i - p.argStart >= p.ma.size:
        if sink.onMsg != nil: sink.onMsg(p.ma, buf[p.argStart ..< i])
        p.argActive = false
        p.argBuf.setLen(0)
        p.msgActive = false
        p.msgBuf.setLen(0)
        p.state = MSG_END
    of MSG_END:
      case b
      of '\n':
        p.drop = 0
        p.argStart = i + 1
        p.state = OP_START
      else:
        inc i
        continue
    of OP_PLUS:
      case b
      of 'O', 'o': p.state = OP_PLUS_O
      else: return fail(p.state, buf, i)
    of OP_PLUS_O:
      case b
      of 'K', 'k': p.state = OP_PLUS_OK
      else: return fail(p.state, buf, i)
    of OP_PLUS_OK:
      case b
      of '\n':
        if sink.onOK != nil: sink.onOK()
        p.drop = 0
        p.state = OP_START
      else: discard
    of OP_MINUS:
      case b
      of 'E', 'e': p.state = OP_MINUS_E
      else: return fail(p.state, buf, i)
    of OP_MINUS_E:
      case b
      of 'R', 'r': p.state = OP_MINUS_ER
      else: return fail(p.state, buf, i)
    of OP_MINUS_ER:
      case b
      of 'R', 'r': p.state = OP_MINUS_ERR
      else: return fail(p.state, buf, i)
    of OP_MINUS_ERR:
      case b
      of ' ', '\t': p.state = OP_MINUS_ERR_SPC
      else: return fail(p.state, buf, i)
    of OP_MINUS_ERR_SPC:
      case b
      of ' ', '\t':
        inc i
        continue
      else:
        p.state = MINUS_ERR_ARG
        p.argStart = i
    of MINUS_ERR_ARG:
      case b
      of '\r':
        p.drop = 1
      of '\n':
        var arg: string
        if p.argActive:
          arg = p.argBuf
          p.argActive = false
          p.argBuf.setLen(0)
        else:
          arg = buf[p.argStart ..< (i - p.drop)]
        if sink.onErr != nil: sink.onErr(arg)
        p.drop = 0
        p.argStart = i + 1
        p.state = OP_START
      else:
        if p.argActive: p.argBuf.add(b)
    of OP_P:
      case b
      of 'I', 'i': p.state = OP_PI
      of 'O', 'o': p.state = OP_PO
      else: return fail(p.state, buf, i)
    of OP_PO:
      case b
      of 'N', 'n': p.state = OP_PON
      else: return fail(p.state, buf, i)
    of OP_PON:
      case b
      of 'G', 'g': p.state = OP_PONG
      else: return fail(p.state, buf, i)
    of OP_PONG:
      case b
      of '\n':
        if sink.onPong != nil: sink.onPong()
        p.drop = 0
        p.state = OP_START
      else: discard
    of OP_PI:
      case b
      of 'N', 'n': p.state = OP_PIN
      else: return fail(p.state, buf, i)
    of OP_PIN:
      case b
      of 'G', 'g': p.state = OP_PING
      else: return fail(p.state, buf, i)
    of OP_PING:
      case b
      of '\n':
        if sink.onPing != nil: sink.onPing()
        p.drop = 0
        p.state = OP_START
      else: discard
    of OP_I:
      case b
      of 'N', 'n': p.state = OP_IN
      else: return fail(p.state, buf, i)
    of OP_IN:
      case b
      of 'F', 'f': p.state = OP_INF
      else: return fail(p.state, buf, i)
    of OP_INF:
      case b
      of 'O', 'o': p.state = OP_INFO
      else: return fail(p.state, buf, i)
    of OP_INFO:
      case b
      of ' ', '\t': p.state = OP_INFO_SPC
      else: return fail(p.state, buf, i)
    of OP_INFO_SPC:
      case b
      of ' ', '\t':
        inc i
        continue
      else:
        p.state = INFO_ARG
        p.argStart = i
    of INFO_ARG:
      case b
      of '\r':
        p.drop = 1
      of '\n':
        var arg: string
        if p.argActive:
          arg = p.argBuf
          p.argActive = false
          p.argBuf.setLen(0)
        else:
          arg = buf[p.argStart ..< (i - p.drop)]
        if sink.onInfo != nil: sink.onInfo(arg)
        p.drop = 0
        p.argStart = i + 1
        p.state = OP_START
      else:
        if p.argActive: p.argBuf.add(b)
    inc i

  # Split-buffer handling, as upstream: keep a partial argument (control line)
  # for the next read...
  if p.state in {MSG_ARG, MINUS_ERR_ARG, INFO_ARG} and not p.argActive:
    p.argActive = true
    p.argBuf.setLen(0)
    p.argBuf.add(buf[p.argStart ..< (buf.len - p.drop)])
  # ...and a partial message payload.
  if p.state == MSG_PAYLOAD and not p.msgActive:
    # Go clones the msgArg here when argBuf is nil; unnecessary, because
    # ma.subject/ma.reply are owned copies in this port.
    p.msgActive = true
    p.msgBuf.setLen(0)
    p.msgBuf.add(buf[p.argStart ..< buf.len])

  ""
