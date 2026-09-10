## Test harness: locate, start and stop a real `nats-server`.
##
## The server is not vendored here (Niffler vendors its own build); the suite
## looks for `$NATS_SERVER_BIN`, then `nats-server` on PATH. When neither
## exists the bus suites print a loud SKIPPED banner and pass — the parser,
## nuid, subject, shim and pending-limit tests need no server.
##
## Configuration goes through a generated config file rather than CLI flags:
## `max_payload` is a config-file-only setting in upstream nats-server (the
## Niffler fork patches the CLI), so `-c` is the portable choice.

import std/[net, os, osproc, strutils, times]

import nats/conn as core

type
  TestServer* = object
    bin*: string
    process: Process
    url*: string
    port*: int
    dir*: string
    logPath*: string

proc serverBinary*(): string =
  ## "" when no server binary is available.
  result = getEnv("NATS_SERVER_BIN")
  if result.len > 0 and fileExists(result): return
  let path = getEnv("PATH")
  for dir in path.split(PathSep):
    if dir.len == 0: continue
    let cand = dir / "nats-server"
    if fileExists(cand): return cand
  result = ""

proc serverAvailable*(): bool = serverBinary().len > 0

proc freePort*(): int =
  let s = newSocket()
  defer: s.close()
  s.bindAddr(Port(0), "127.0.0.1")
  result = int(s.getLocalAddr()[1])

proc startServer*(maxPayload = 1024, extraConfig = ""): TestServer =
  ## Start a private server in its own temp dir. `maxPayload` is small by
  ## default so the client-side guard is exercised without large payloads.
  result.bin = serverBinary()
  if result.bin.len == 0:
    raise newException(IOError, "no nats-server binary available")
  result.port = freePort()
  result.url = "nats://127.0.0.1:" & $result.port
  result.dir = getTempDir() / ("natsnim-test-" & $getCurrentProcessId())
  createDir(result.dir)
  result.logPath = result.dir / "server.log"
  let cfgPath = result.dir / "server.conf"
  var cfg = "host: 127.0.0.1\nport: " & $result.port & "\n"
  if maxPayload > 0:
    cfg.add("max_payload: " & $maxPayload & "\n")
  if extraConfig.len > 0:
    cfg.add(extraConfig)
  writeFile(cfgPath, cfg)

  stderr.writeLine("[harness] starting " & result.bin & " on port " & $result.port)
  result.process = startProcess(result.bin, args = @["-c", cfgPath, "-l", result.logPath],
                             options = {poUsePath, poDaemon})

  # Readiness: the handshake is the probe, so a partially started server is
  # never mistaken for a ready one.
  let deadline = epochTime() + 10.0
  var lastErr = ""
  while epochTime() < deadline:
    try:
      let c = core.dial(result.url)
      c.close()
      stderr.writeLine("[harness] ready at " & result.url)
      return
    except CatchableError as e:
      lastErr = e.msg
      sleep(50)
  let log = if fileExists(result.logPath): readFile(result.logPath) else: "(no log)"
  raise newException(IOError, "nats-server did not become ready at " &
    result.url & " (last: " & lastErr & ")\n--- server log ---\n" & log)

proc stopProcess*(p: Process) =
  ## terminate, then kill if it does not go within 2s. std/osproc's
  ## waitForExit returns the exit code (and -1 on timeout), so poll `running`.
  if p == nil: return
  try:
    p.terminate()
    var waited = 0
    while p.running() and waited < 2000:
      sleep(20)
      waited += 20
    if p.running(): p.kill()
  except CatchableError:
    discard
  p.close()

proc stop*(srv: TestServer) =
  stopProcess(srv.process)
  if srv.dir.len > 0:
    try: removeDir(srv.dir)
    except CatchableError: discard

proc startResponder*(url, subject: string, prefix = ""): Process =
  ## Spawn the echo-responder fixture and wait until it is subscribed.
  ## `request()` blocks while waiting for the reply, so the other party has to
  ## be a separate process.
  let bin = getAppDir() / "responder"
  if not fileExists(bin):
    raise newException(IOError, "responder fixture not built at " & bin)
  let ready = getTempDir() / ("natsnim-responder-" &
    $getCurrentProcessId() & "-" & $freePort())
  removeFile(ready)
  var args = @[url, subject]
  if prefix.len > 0: args.add(prefix)
  args.add("--ready")
  args.add(ready)
  result = startProcess(bin, args = args, options = {poUsePath, poDaemon})
  let deadline = epochTime() + 8.0
  while not fileExists(ready):
    if epochTime() > deadline:
      stopProcess(result)
      raise newException(IOError, "responder never became ready")
    sleep(25)
  removeFile(ready)

proc stopResponder*(p: Process) =
  stopProcess(p)

proc skipBanner*() =
  echo ""
  echo "  ***************************************************************"
  echo "  * SKIPPED: no nats-server found.                              *"
  echo "  * Set NATS_SERVER_BIN=/path/to/nats-server (or put it on PATH) *"
  echo "  * to run the core-NATS transport tests.                       *"
  echo "  ***************************************************************"
  echo ""
