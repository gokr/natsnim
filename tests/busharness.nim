## Private, restartable real-server fixtures. NATS_SERVER_BIN overrides PATH.
## NATS_REQUIRE_SERVER=1 makes missing integration prerequisites a test failure.

import std/[json, net, os, osproc, tempfiles, times]
import natsnim/conn as core

type TestServer* = object
  bin*: string
  process: Process
  url*: string
  port*: int
  dir*, logPath*, cfgPath*: string

proc serverBinary*(): string =
  let configured = getEnv("NATS_SERVER_BIN")
  if configured.len > 0:
    if not fileExists(configured):
      raise newException(IOError, "NATS_SERVER_BIN does not exist")
    return configured
  findExe("nats-server")
proc serverAvailable*(): bool = serverBinary().len > 0

proc freePort*(): int =
  ## Only for legacy external probes; real fixtures use server-assigned ports.
  let s = newSocket()
  defer: s.close()
  s.bindAddr(Port(0), "127.0.0.1")
  int(s.getLocalAddr()[1])

proc stopProcess*(p: Process) =
  if p == nil: return
  try:
    if p.running():
      p.terminate()
      if p.waitForExit(2000) == -1 and p.running():
        p.kill()
        discard p.waitForExit(2000)
  finally: p.close()

proc startServerProcess*(srv: var TestServer) =
  ## The ports file and live child are an independent readiness oracle: do not
  ## rely solely on the client implementation whose handshake we are testing.
  for path in walkFiles(srv.dir / "*.ports"): removeFile(path)
  srv.process = startProcess(srv.bin, args = @["-c", srv.cfgPath,
    "-p", $srv.port, "-l", srv.logPath, "--ports_file_dir", srv.dir],
    options = {poUsePath, poDaemon})
  try:
    let deadline = epochTime() + 10.0
    while epochTime() < deadline:
      if not srv.process.running(): break
      for path in walkFiles(srv.dir / "*.ports"):
        let ports = parseFile(path)
        if ports{"nats"} != nil and ports["nats"].len > 0:
          srv.url = ports["nats"][0].getStr()
          srv.port = core.parseUrl(srv.url).port
          return
      sleep(20)
    let log = if fileExists(srv.logPath): readFile(srv.logPath) else: "(no log)"
    raise newException(IOError, "nats-server did not become ready\n" & log)
  except:
    stopProcess(srv.process)
    srv.process = nil
    raise

proc stopServerProcess*(srv: var TestServer) =
  stopProcess(srv.process)
  srv.process = nil
proc restart*(srv: var TestServer) =
  srv.stopServerProcess()
  srv.startServerProcess()
proc startServer*(maxPayload = 1024, extraConfig = ""): TestServer =
  result.bin = serverBinary()
  if result.bin.len == 0: raise newException(IOError, "no nats-server available")
  result.port = -1
  result.dir = createTempDir("natsnim-test-", "")
  result.logPath = result.dir / "server.log"
  result.cfgPath = result.dir / "server.conf"
  var cfg = "host: 127.0.0.1\n"
  if maxPayload > 0: cfg.add("max_payload: " & $maxPayload & "\n")
  cfg.add(extraConfig)
  writeFile(result.cfgPath, cfg)
  try: result.startServerProcess()
  except:
    removeDir(result.dir)
    raise
proc stop*(srv: TestServer) =
  stopProcess(srv.process)
  if srv.dir.len > 0: removeDir(srv.dir)

proc startResponder*(url, subject: string, prefix = ""): Process =
  let bin = getAppDir() / "responder"
  if not fileExists(bin): raise newException(IOError, "responder fixture missing")
  let dir = createTempDir("natsnim-responder-", "")
  defer: removeDir(dir)
  let ready = dir / "ready"
  var args = @[url, subject]
  if prefix.len > 0: args.add(prefix)
  args.add(["--ready", ready])
  result = startProcess(bin, args = args, options = {poUsePath, poDaemon})
  try:
    let deadline = epochTime() + 8.0
    while not fileExists(ready):
      if not result.running() or epochTime() > deadline:
        raise newException(IOError, "responder never became ready")
      sleep(20)
  except:
    stopProcess(result)
    raise
proc stopResponder*(p: Process) = stopProcess(p)
proc skipBanner*() =
  if getEnv("NATS_REQUIRE_SERVER") == "1":
    raise newException(IOError, "required nats-server is missing")
  echo "SKIPPED real-server tests: set NATS_SERVER_BIN or install nats-server."
