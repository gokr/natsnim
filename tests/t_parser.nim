## Parser tests, ported from nats-io/nats.go `nats_test.go` @ 1ffb90b
## (Apache-2.0): TestParserPing, TestParserErr, TestParserOK,
## TestParserShouldFail, TestParserSplitMsg — plus INFO and HMSG coverage.
##
## Where upstream asserts on `Conn.Statistics`, the recording sink counts
## (the parser owns no statistics; see the module header in nats/parser.nim).
## Where upstream asserts `ps.argBuf != nil`, this uses `hasArgBuf()`.

import std/[strutils, unittest]
import natsnim/parser

type
  Rec = object
    msgs: seq[tuple[subject, reply, payload: string, sid: int64, hdr, size: int]]
    oks, pings, pongs, errs, infos: int
    lastErr, lastInfo: string
    inMsgs: uint64
    inBytes: uint64

var rec: Rec

proc recMsg(a: MsgArgs, payload: string) =
  rec.msgs.add((a.subject, a.reply, payload, a.sid, a.hdr, a.size))
  inc rec.inMsgs
  rec.inBytes += uint64(payload.len)

proc recErr(text: string) =
  inc rec.errs
  rec.lastErr = text

proc recInfo(json: string) =
  inc rec.infos
  rec.lastInfo = json

proc recOK() = inc rec.oks
proc recPing() = inc rec.pings
proc recPong() = inc rec.pongs

proc sink(): Sink =
  Sink(onMsg: recMsg, onOK: recOK, onErr: recErr, onPing: recPing,
       onPong: recPong, onInfo: recInfo)

proc reset() =
  rec = Rec()

const noErr = ""

suite "parser":

  test "PING, byte by byte (TestParserPing)":
    reset()
    var p = initParser()
    check p.state == OP_START

    const ping = "PING\r\n"
    check p.parse(ping[0 ..< 1], sink()) == noErr and p.state == OP_P
    check p.parse(ping[1 ..< 2], sink()) == noErr and p.state == OP_PI
    check p.parse(ping[2 ..< 3], sink()) == noErr and p.state == OP_PIN
    check p.parse(ping[3 ..< 4], sink()) == noErr and p.state == OP_PING
    check p.parse(ping[4 ..< 5], sink()) == noErr and p.state == OP_PING
    check p.parse(ping[5 ..< 6], sink()) == noErr and p.state == OP_START
    check rec.pings == 1

    check p.parse(ping, sink()) == noErr and p.state == OP_START
    check rec.pings == 2

    # Should tolerate spaces
    check p.parse("PING  \r", sink()) == noErr and p.state == OP_PING
    p.state = OP_START
    check p.parse("PING  \r  \n", sink()) == noErr and p.state == OP_START
    check rec.pings == 3

  test "PONG":
    reset()
    var p = initParser()
    check p.parse("PONG\r\n", sink()) == noErr and p.state == OP_START
    check rec.pongs == 1
    # PONG is tolerant for anything between PONG and \n
    check p.parse("PONx", sink()) != noErr

  test "ERR, byte by byte and with a split arg buffer (TestParserErr)":
    reset()
    var p = initParser()
    check p.state == OP_START

    const expectedError = "'Any kind of error'"
    const errProto = "-ERR  'Any kind of error'\r\n"

    check p.parse(errProto[0 ..< 1], sink()) == noErr and p.state == OP_MINUS
    check p.parse(errProto[1 ..< 2], sink()) == noErr and p.state == OP_MINUS_E
    check p.parse(errProto[2 ..< 3], sink()) == noErr and p.state == OP_MINUS_ER
    check p.parse(errProto[3 ..< 4], sink()) == noErr and p.state == OP_MINUS_ERR
    check p.parse(errProto[4 ..< 5], sink()) == noErr and p.state == OP_MINUS_ERR_SPC
    check p.parse(errProto[5 ..< 6], sink()) == noErr and p.state == OP_MINUS_ERR_SPC

    # Split arg buffer
    check p.parse(errProto[6 ..< 7], sink()) == noErr and p.state == MINUS_ERR_ARG
    check p.parse(errProto[7 ..< 10], sink()) == noErr and p.state == MINUS_ERR_ARG
    check p.parse(errProto[10 ..< errProto.len - 2], sink()) == noErr
    check p.state == MINUS_ERR_ARG
    check p.hasArgBuf
    check p.argBuf == expectedError

    check p.parse(errProto[errProto.len - 2 ..< errProto.len], sink()) == noErr
    check p.state == OP_START
    check rec.errs == 1
    check rec.lastErr == expectedError

    # Without a split arg buffer
    check p.parse("-ERR 'Any error'\r\n", sink()) == noErr and p.state == OP_START
    check rec.errs == 2
    check rec.lastErr == "'Any error'"

  test "+OK, byte by byte (TestParserOK)":
    reset()
    var p = initParser()
    check p.state == OP_START
    const okProto = "+OKay\r\n"
    check p.parse(okProto[0 ..< 1], sink()) == noErr and p.state == OP_PLUS
    check p.parse(okProto[1 ..< 2], sink()) == noErr and p.state == OP_PLUS_O
    check p.parse(okProto[2 ..< 3], sink()) == noErr and p.state == OP_PLUS_OK
    check p.parse(okProto[3 ..< okProto.len], sink()) == noErr and p.state == OP_START
    check rec.oks == 1

  test "malformed input fails (TestParserShouldFail)":
    reset()
    var p = initParser()
    const bad = [
      " PING", "POO", "Px", "PIx", "PINx",
      "POx", "PONx", "ZOO", "Mx\r\n", "MSx\r\n", "MSGx\r\n",
      "MSG  foo\r\n", "MSG \r\n", "MSG foo 1\r\n", "MSG foo bar 1\r\n",
      "MSG foo bar 1 baz\r\n", "MSG foo 1 bar baz\r\n",
      "+x\r\n", "+Ox\r\n", "-x\r\n", "-Ex\r\n", "-ERx\r\n", "-ERRx\r\n",
      "Hx\r\n", "Ix\r\n", "INx\r\n",
    ]
    for bad in bad:
      p = initParser()
      check p.parse(bad, sink()) != noErr

  test "split MSG (TestParserSplitMsg)":
    reset()
    var p = initParser()

    # Bad argument lines must fail.
    check p.parse("MSG a\r\n", sink()) != noErr
    p = initParser()
    check p.parse("MSG a b c\r\n", sink()) != noErr
    p = initParser()

    var expectedCount = 1'u64
    var expectedSize = 3'u64

    check p.parse("MSG a", sink()) == noErr
    check p.hasArgBuf

    check p.parse(" 1 3\r\nf", sink()) == noErr
    check p.ma.size == 3
    check p.ma.sid == 1
    check p.ma.subject == "a"
    check p.hasMsgBuf

    check p.parse("oo\r\n", sink()) == noErr
    check rec.inMsgs == expectedCount
    check rec.inBytes == expectedSize
    check not p.hasArgBuf
    check not p.hasMsgBuf
    check rec.msgs[^1].payload == "foo"

    check p.parse("MSG a 1 3\r\nfo", sink()) == noErr
    check p.ma.size == 3
    check p.ma.sid == 1
    check p.ma.subject == "a"
    # Upstream asserts argBuf != nil here: it points ma.subject/reply into
    # scratch to escape the read buffer when a payload is split. Owned strings
    # make that unnecessary, so only the pending payload is tracked.
    check not p.hasArgBuf
    check p.hasMsgBuf

    expectedCount += 1
    expectedSize += 3

    check p.parse("o\r\n", sink()) == noErr
    check rec.inMsgs == expectedCount
    check rec.inBytes == expectedSize
    check not p.hasArgBuf
    check not p.hasMsgBuf

    check p.parse("MSG a 1 6\r\nfo", sink()) == noErr
    check p.ma.size == 6
    check p.ma.sid == 1
    check p.ma.subject == "a"
    check not p.hasArgBuf   # see the note above
    check p.hasMsgBuf

    check p.parse("ob", sink()) == noErr
    expectedCount += 1
    expectedSize += 6
    check p.parse("ar\r\n", sink()) == noErr
    check rec.inMsgs == expectedCount
    check rec.inBytes == expectedSize
    check not p.hasArgBuf
    check not p.hasMsgBuf
    check rec.msgs[^1].payload == "foobar"

    # A message bigger than the upstream scratch buffer (4 KiB): the payload
    # arrives in pieces and must be reassembled byte-exactly.
    let msgSize = MAX_CONTROL_LINE_SIZE + 100 + 3
    check p.parse("MSG a 1 b " & $msgSize & "\r\nfoo", sink()) == noErr
    check p.ma.size == msgSize
    check p.ma.sid == 1
    check p.ma.subject == "a"
    check p.ma.reply == "b"
    check not p.hasArgBuf   # see the note above
    check p.hasMsgBuf

    expectedCount += 1
    expectedSize += uint64(msgSize)

    var buf = newString(msgSize - 3)
    for i in 0 ..< buf.len:
      buf[i] = char(ord('a') + (i mod 26))

    check p.parse(buf, sink()) == noErr
    check p.state == MSG_PAYLOAD
    check p.ma.size == msgSize
    check p.msgBuf.len == msgSize
    check p.msgBuf[0 ..< 3] == "foo"
    for k in 3 ..< p.ma.size:
      check p.msgBuf[k] == char(ord('a') + ((k - 3) mod 26))

    check p.parse("\r\n", sink()) == noErr
    check rec.inMsgs == expectedCount
    check rec.inBytes == expectedSize
    check not p.hasArgBuf
    check not p.hasMsgBuf
    check p.state == OP_START
    check rec.msgs[^1].payload.len == msgSize
    check rec.msgs[^1].payload[0 ..< 3] == "foo"
    check rec.msgs[^1].payload[3 ..< 3 + 26] == "abcdefghijklmnopqrstuvwxyz"

  test "async INFO with a split arg":
    reset()
    var p = initParser()
    const info = """INFO {"server_id":"x","max_payload":1048576}""" & "\r\n"
    check p.parse(info[0 ..< 6], sink()) == noErr  # "INFO {": arg starts
    check p.hasArgBuf
    check p.parse(info[6 ..< info.len], sink()) == noErr
    check p.state == OP_START
    check not p.hasArgBuf
    check rec.infos == 1
    check rec.lastInfo.startsWith("{\"server_id\"")

  test "HMSG carries the header length (headers stay in the payload)":
    reset()
    var p = initParser()
    # headers block "NATS/1.0\r\n\r\n" is 12 bytes; body "hello" is 5.
    check p.parse("HMSG foo 1 12 17\r\nNATS/1.0\r\n\r\nhello\r\n", sink()) == noErr
    check p.state == OP_START
    check rec.msgs.len == 1
    check rec.msgs[0].subject == "foo"
    check rec.msgs[0].sid == 1
    check rec.msgs[0].hdr == 12
    check rec.msgs[0].size == 17
    check rec.msgs[0].reply == ""
    check rec.msgs[0].payload == "NATS/1.0\r\n\r\nhello"

  test "HMSG with a reply subject, split across reads":
    reset()
    var p = initParser()
    const hmsg = "HMSG foo 7 _INBOX.abc 12 17\r\nNATS/1.0\r\n\r\nhello\r\n"
    for k in 0 ..< hmsg.len:
      check p.parse(hmsg[k ..< k + 1], sink()) == noErr
    check rec.msgs.len == 1
    check rec.msgs[0].subject == "foo"
    check rec.msgs[0].reply == "_INBOX.abc"
    check rec.msgs[0].sid == 7
    check rec.msgs[0].hdr == 12
    check rec.msgs[0].payload == "NATS/1.0\r\n\r\nhello"

  test "HMSG rejects an out-of-range header size":
    reset()
    var p = initParser()
    # hdr (20) > size (17)
    check p.parse("HMSG foo 1 20 17\r\n", sink()) != noErr
    p = initParser()
    check p.parse("HMSG foo 1 12\r\n", sink()) != noErr  # too few args

  test "MSG with a reply and an empty subject token is rejected":
    reset()
    var p = initParser()
    check p.parse("MSG 1 3\r\nfoo\r\n", sink()) != noErr
