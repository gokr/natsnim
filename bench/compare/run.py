#!/usr/bin/env python3
"""Head-to-head client benchmark: nats.go (Go) vs natsnim (Nim), same server.

Builds both peers on first use, starts a private nats-server, and runs the
scenario matrix: request/reply at three payload sizes, publish fan-out
throughput across publisher/subscriber pairings, and connect/handshake cost.

Go's `Publish` is batched by its flusher goroutine; the Nim equivalent is the
explicit batch API (`deferFlush`/`flushOutbound`, the `batch` template), so
the throughput matrix compares "nim(pub)" (write-through default) against
"nim(pubbatch)" (batched) and both against Go's publisher.

Usage:  python3 bench/compare/run.py
Env:    NATS_SERVER_BIN  path to a nats-server binary (default: PATH)
        NATS_BENCH_PORT  port for the private server (default 43333)

Results are hardware- and load-sensitive: compare ratios within one run,
never absolute numbers across runs or machines.
"""
import os
import pathlib
import shutil
import statistics
import subprocess
import time

ROOT = pathlib.Path(__file__).resolve().parent
REPO = ROOT.parent.parent
NIM = ROOT / "bin" / "nimbench"
GO = ROOT / "gobench" / "gobench"
CLIENTS = {"nim": str(NIM), "go": str(GO)}
PORT = int(os.environ.get("NATS_BENCH_PORT", "43333"))
URL = f"nats://127.0.0.1:{PORT}"
COUNT_TP = 20000


def find_server():
    server = os.environ.get("NATS_SERVER_BIN") or shutil.which("nats-server")
    if not server:
        raise SystemExit("no nats-server found; set NATS_SERVER_BIN or PATH")
    return server


def ensure_peers():
    NIM.parent.mkdir(parents=True, exist_ok=True)
    if not NIM.exists():
        subprocess.run(["nim", "c", "-d:release", "--hints:off",
                        f"--path:{REPO / 'src'}", f"-o:{NIM}",
                        str(ROOT / "nimbench.nim")], check=True)
    if not GO.exists():
        subprocess.run(["go", "build", "-o", str(GO), "."],
                       cwd=ROOT / "gobench", check=True)


def run_pair(req_client, resp_client, subject, count, size):
    """Request/reply: start a responder, wait READY, time `count` requests."""
    resp = subprocess.Popen([CLIENTS[resp_client], "resp", URL, subject],
                            stdout=subprocess.PIPE, text=True)
    assert resp.stdout.readline().strip() == "READY"
    out = subprocess.run([CLIENTS[req_client], "req", URL, subject,
                          str(count), str(size)],
                         capture_output=True, text=True, timeout=120)
    resp.kill()
    return int(out.stdout.split()[1])


def run_throughput(pub_client, sub_client, subject, count, size,
                   pub_role="pub"):
    """Publish/count messages as fast as possible; time the SUB side."""
    sub = subprocess.Popen([CLIENTS[sub_client], "sub", URL, subject,
                            str(count)], stdout=subprocess.PIPE, text=True)
    assert sub.stdout.readline().strip() == "READY"
    subprocess.run([CLIENTS[pub_client], pub_role, URL, subject,
                    str(count), str(size)],
                   capture_output=True, text=True, timeout=120)
    line = sub.stdout.readline()
    sub.wait(timeout=10)
    return int(line.split()[1])


def run_dial(client, count):
    out = subprocess.run([CLIENTS[client], "dial", URL, str(count)],
                         capture_output=True, text=True, timeout=120)
    return int(out.stdout.split()[1])


def main():
    ensure_peers()
    server = find_server()
    cfg = ROOT / "bench.conf"
    cfg.write_text(f"host: 127.0.0.1\nport: {PORT}\nmax_payload: 4194304\n")
    proc = subprocess.Popen([server, "-c", str(cfg)],
                            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    time.sleep(0.5)
    results = {}

    def record(name, value):
        results.setdefault(name, []).append(value)

    try:
        for r in range(3):
            for resp_c in ("nim", "go"):
                for req_c in ("nim", "go"):
                    us = run_pair(req_c, resp_c,
                                  f"bench.req.{resp_c}.{req_c}.{r}", 3000, 128)
                    record(f"req128 {req_c}req/{resp_c}resp", us)
            for size, tag, cnt in ((65536, "req64k", 400),
                                   (524288, "req512k", 100)):
                for c in ("nim", "go"):
                    record(f"{tag} {c}",
                           run_pair(c, c, f"bench.{tag}.{c}.{r}", cnt, size))
            for pub_c, sub_c, role in (
                    ("go", "go", "pub"), ("go", "nim", "pub"),
                    ("nim", "go", "pubbatch"), ("nim", "nim", "pubbatch"),
                    ("nim", "nim", "pub")):
                record(f"tp256 {pub_c}({role})/{sub_c}sub",
                       run_throughput(pub_c, sub_c,
                                      f"bench.tpx.{pub_c}.{sub_c}.{role}.{r}",
                                      COUNT_TP, 256, role))
            for c in ("nim", "go"):
                record(f"dial {c}", run_dial(c, 100))

        for name, vals in sorted(results.items()):
            m = statistics.median(vals)
            kind = name.split()[0]
            if kind == "tp256":
                mps = COUNT_TP * 1e6 / m
                print(f"{name:40} {mps:>12,.0f} msgs/s")
            elif kind.startswith("req"):
                per = m / {"req128": 3000, "req64k": 400, "req512k": 100}[kind]
                print(f"{name:40} {per:>10.1f} us/req")
            elif kind == "dial":
                print(f"{name:40} {m/100:>10.0f} us/dial")
    finally:
        proc.terminate()


main()
