## Fuzz/property tests for the parser — not present upstream.
##
## Upstream's split tests (TestParserSplitMsg, the byte-by-byte cases in
## TestParserErr) are hand-written and cover a handful of boundaries. The
## property that actually matters for a stream parser is stronger:
##
##   for any valid byte stream S, the sequence of operations produced is
##   identical no matter how S is chopped into reads.
##
## A socket read can split anywhere — including inside a `MSG` argument line,
## inside a payload, or one byte before `\r\n` — so this is exercised with
## randomized frame sequences and randomized chunk sizes, from 1 byte upward.
## Payloads deliberately contain '\r', '\n', "PING\r\n" and "MSG " sequences:
## framing is size-delimited, so those bytes must be delivered verbatim and
## never interpreted.

import std/[random, strutils, unittest]
import natsnim/parser

type
  OpKind = enum okMsg, okOK, okErr, okPing, okPong, okInfo
  Op = object
    kind: OpKind
    subject, reply, payload, text: string
    sid: int64
    hdr, size: int

var ops: seq[Op]

proc sMsg(a: MsgArgs, payload: string) =
  doAssert payload.len == a.size, "payload/size mismatch"
  ops.add(Op(kind: okMsg, subject: a.subject, reply: a.reply,
             payload: payload, sid: a.sid, hdr: a.hdr, size: a.size))

proc sOK() = ops.add(Op(kind: okOK))
proc sErr(t: string) = ops.add(Op(kind: okErr, text: t))
proc sPing() = ops.add(Op(kind: okPing))
proc sPong() = ops.add(Op(kind: okPong))
proc sInfo(j: string) = ops.add(Op(kind: okInfo, text: j))

proc sink(): Sink =
  Sink(onMsg: sMsg, onOK: sOK, onErr: sErr, onPing: sPing, onPong: sPong,
       onInfo: sInfo)

proc feed(stream: string, chunk: int): seq[Op] =
  ## Parse `stream` in `chunk`-sized reads and return the operations.
  ops = @[]
  var p = initParser()
  var i = 0
  while i < stream.len:
    let n = min(chunk, stream.len - i)
    let e = p.parse(stream[i ..< i + n], sink())
    doAssert e.len == 0, "unexpected parse error: " & e
    i += n
  check p.state == OP_START
  ops

proc `==`(a, b: Op): bool =
  a.kind == b.kind and a.subject == b.subject and a.reply == b.reply and
    a.payload == b.payload and a.text == b.text and a.sid == b.sid and
    a.hdr == b.hdr and a.size == b.size

proc `$`(o: Op): string =
  case o.kind
  of okMsg: "msg(" & o.subject & ",sid=" & $o.sid & ",reply=" & o.reply &
            ",hdr=" & $o.hdr & ",size=" & $o.size & ",payload=" & o.payload.escape & ")"
  of okOK: "+OK"
  of okErr: "-ERR " & o.text.escape
  of okPing: "PING"
  of okPong: "PONG"
  of okInfo: "INFO " & o.text.escape

const subjectChars = "abc._-0123456789"
const payloadChars = "ab\r\nPING MSG +OK -ERR info.{}"
const errTextChars = "abPING MSG +OK -ERR info.{}"

proc randToken(r: var Rand; minLen, maxLen: int; alphabet: string): string =
  let n = r.rand(minLen .. maxLen)
  result = newString(n)
  for i in 0 ..< n: result[i] = alphabet[r.rand(alphabet.high)]

proc strippedLeadingWs(s: string): string =
  ## The -ERR/INFO `_SPC` states skip every leading space and tab before the
  ## argument starts, so leading whitespace is not part of the text. Modelling
  ## that keeps the generator honest instead of encoding the quirk as noise.
  var i = 0
  while i < s.len and s[i] in {' ', '\t'}: inc i
  result = s[i .. ^1]
  if result.len == 0: result = "x"

proc genStream(r: var Rand; frames: int): (string, seq[Op]) =
  ## Build a valid wire stream and the operations it must produce.
  var expected: seq[Op] = @[]
  var wire = ""
  for _ in 0 ..< frames:
    case r.rand(6)
    of 0:
      wire.add("PING\r\n"); expected.add(Op(kind: okPing))
    of 1:
      wire.add("PONG\r\n"); expected.add(Op(kind: okPong))
    of 2:
      wire.add("+OK\r\n"); expected.add(Op(kind: okOK))
    of 3:
      # min length 1: upstream (and this port) parse an empty `-ERR \r\n` as
      # the text "\r" — the \r enters MINUS_ERR_ARG as the arg's first byte.
      # That quirk is pinned explicitly in the tests below, not fuzzed.
      let t = strippedLeadingWs(randToken(r, 1, 12, errTextChars))
      wire.add("-ERR " & t & "\r\n"); expected.add(Op(kind: okErr, text: t))
    of 4:
      let j = strippedLeadingWs("{\"id\":" & $r.rand(1000) & "}")
      wire.add("INFO " & j & "\r\n"); expected.add(Op(kind: okInfo, text: j))
    else:
      let subject = randToken(r, 1, 8, subjectChars)
      let sid = int64(r.rand(1 .. 999))
      let payload = randToken(r, 0, 40, payloadChars)
      let withReply = r.rand(1) == 0
      let hms = r.rand(3) == 0
      if hms:
        # headers + body; the parser delivers the whole block verbatim
        let hdrs = "NATS/1.0\r\n" & (if r.rand(1) == 0: "X: y\r\n" else: "") & "\r\n"
        let body = payload
        let size = hdrs.len + body.len
        let full = hdrs & body
        if withReply:
          wire.add("HMSG " & subject & " " & $sid & " _INBOX.x " & $hdrs.len &
                   " " & $size & "\r\n" & full & "\r\n")
        else:
          wire.add("HMSG " & subject & " " & $sid & " " & $hdrs.len & " " &
                   $size & "\r\n" & full & "\r\n")
        expected.add(Op(kind: okMsg, subject: subject, sid: sid,
                        reply: (if withReply: "_INBOX.x" else: ""),
                        hdr: hdrs.len, size: size, payload: full))
      else:
        if withReply:
          wire.add("MSG " & subject & " " & $sid & " _INBOX.x " & $payload.len &
                   "\r\n" & payload & "\r\n")
        else:
          wire.add("MSG " & subject & " " & $sid & " " & $payload.len & "\r\n" &
                   payload & "\r\n")
        expected.add(Op(kind: okMsg, subject: subject, sid: sid,
                        reply: (if withReply: "_INBOX.x" else: ""),
                        hdr: -1, size: payload.len, payload: payload))
  (wire, expected)

suite "parser fuzz":
  test "operation sequence is invariant under random chunking":
    var r = initRand(20260910)
    var streams = 0
    for _ in 0 ..< 300:
      let frames = r.rand(1 .. 12)
      let (wire, expected) = genStream(r, frames)
      streams += 1
      # single-shot, tiny reads, and random reads must all agree
      check feed(wire, wire.len) == expected
      check feed(wire, 1) == expected
      check feed(wire, 2) == expected
      check feed(wire, 3) == expected
      var i = 0
      ops = @[]
      var p = initParser()
      var got: seq[Op] = @[]
      while i < wire.len:
        let n = min(r.rand(1 .. 17), wire.len - i)
        let e = p.parse(wire[i ..< i + n], sink())
        doAssert e.len == 0, "unexpected parse error: " & e
        i += n
      check p.state == OP_START
      check ops == expected
    check streams == 300

  test "payload bytes are never interpreted as control frames":
    # Size-delimited framing: a payload that *looks* like frames must arrive
    # whole, and the frames after it must still parse.
    ops = @[]
    var p = initParser()
    const payload = "PING\r\n+OK\r\nMSG x 1 5\r\nhello\r\n"
    let wire = "MSG t 9 " & $payload.len & "\r\n" & payload &
               "\r\nPING\r\n"
    check p.parse(wire, sink()) == ""
    check ops.len == 2
    check ops[0].kind == okMsg
    check ops[0].payload == payload
    check ops[0].size == payload.len
    check ops[1].kind == okPing

  test "a frame split at every possible boundary parses identically":
    # Exhaustive for one representative stream: every 2-way split point.
    const wire = "MSG alpha 42 _INBOX.q " & "0" & "" &
                 "7\r\npayload\r\nPING\r\n+OK\r\n"
    let single = feed(wire, wire.len)
    check single.len == 3
    for cut in 1 ..< wire.len:
      ops = @[]
      var p = initParser()
      check p.parse(wire[0 ..< cut], sink()) == ""
      check p.parse(wire[cut ..< wire.len], sink()) == ""
      check p.state == OP_START
      check ops == single

  test "empty -ERR/INFO arguments reproduce upstream's quirks":
    # No upstream test covers these. They are documented because they look like
    # bugs and are not: the `_SPC` states skip only ' ' and '\t', so with
    # `-ERR \r\n` the \r itself becomes the argument's first byte and the text
    # is "\r". A tab before it changes nothing (it is skipped, then \r is the
    # arg). An *empty* error text is therefore unreachable. This port matches
    # nats.go's OP_MINUS_ERR_SPC exactly.
    for wire in ["-ERR \r\n", "-ERR \t\r\n"]:
      ops = @[]
      var p = initParser()
      check p.parse(wire, sink()) == ""
      check ops.len == 1
      check ops[0].kind == okErr
      check ops[0].text == "\r"

    ops = @[]
    var p = initParser()
    check p.parse("INFO \r\n", sink()) == ""
    check ops.len == 1 and ops[0].kind == okInfo and ops[0].text == "\r"

    # And the converse: leading whitespace is skipped, so text never carries it.
    ops = @[]
    p = initParser()
    check p.parse("-ERR    spaced\r\n", sink()) == ""
    check ops.len == 1 and ops[0].text == "spaced"

    ops = @[]
    p = initParser()
    check p.parse("INFO \t{\"a\":1}\r\n", sink()) == ""
    check ops.len == 1 and ops[0].text == "{\"a\":1}"

  test "a zero-length payload is delivered":
    ops = @[]
    var p = initParser()
    check p.parse("MSG a.b 1 0\r\n\r\n", sink()) == ""
    check ops.len == 1
    check ops[0].kind == okMsg
    check ops[0].subject == "a.b"
    check ops[0].sid == 1
    check ops[0].size == 0
    check ops[0].payload == ""
    check p.state == OP_START

  test "a payload of exactly the maximum sizes upstream tests is intact":
    # Size-delimited framing must not care about the content: 8 KiB of the
    # same byte, including the `\r\n` that terminates the frame.
    let n = 8192
    let payload = repeat("\r\n", n div 2)
    ops = @[]
    var p = initParser()
    check p.parse("MSG big 3 " & $payload.len & "\r\n" & payload & "\r\n",
                  sink()) == ""
    check ops.len == 1
    check ops[0].payload == payload
    check ops[0].size == n
    check p.state == OP_START
