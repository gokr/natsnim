#!/usr/bin/env python3
"""Deterministic adversarial peers; no external server, sleeps only at faults.
Python owns peer threads; the Nim client itself remains synchronous/threadless.
Every client has an outer watchdog, so regressions cannot hang the test runner.
"""
import pathlib
import socket
import subprocess
import sys
import threading
import time

BIN = str(pathlib.Path(__file__).parent / "bin" / "faultclient")


def run(mode):
    listener = socket.socket()
    listener.bind(("127.0.0.1", 0))
    listener.listen()
    listener.settimeout(5)
    port = listener.getsockname()[1]
    clients = []
    errors = []

    def accept():
        s, _ = listener.accept()
        s.settimeout(4)
        clients.append(s)
        return s

    def handshake(s, auth_error=False):
        s.sendall(b'INFO {"max_payload":16777216,"headers":true}\r\n')
        f = s.makefile("rb")
        assert f.readline().startswith(b"CONNECT ")
        assert f.readline() == b"PING\r\n"
        s.sendall(b"-ERR 'Authorization Violation'\r\n" if auth_error else b"PONG\r\n")
        return f

    def server():
        try:
            if mode == "failed-dial":
                for _ in range(10):
                    accept()  # intentionally never send INFO
                return
            s = accept()
            f = handshake(s, mode == "auth")
            if mode == "auth":
                f.close()
                return
            if mode == "deadline":
                time.sleep(0.02)
                f.close()
                s.close()
                s = accept()
                time.sleep(0.12)
                f = handshake(s)
            if mode == "write":
                time.sleep(0.7)  # never read the body, then reset/close
                f.close()
                return
            sent_error = False
            while True:
                line = f.readline()
                if not line:
                    break
                parts = line.split()
                if parts[0] == b"PING":
                    s.sendall(b"PONG\r\n")
                elif parts[0] == b"SUB":
                    if mode == "ping":
                        s.sendall(b"PING\r\n")
                    elif mode == "malformed":
                        s.sendall(b"MSG t 1 9223372036854775807\r\n")
                elif parts[0] == b"PONG" and mode == "ping":
                    s.sendall(b"MSG t 1 9\r\npong-seen\r\n")
                elif parts[0] == b"PUB":
                    size = int(parts[-1])
                    if mode == "partial":
                        body = bytearray()
                        while len(body) < size:
                            body.extend(f.read(min(16384, size - len(body))))
                            time.sleep(0.0005)  # force send backpressure/partial writes
                        assert f.read(2) == b"\r\n"
                        assert body == (bytes(range(251)) * (size // 251 + 1))[:size]
                        reply = parts[-2]
                        s.sendall(b"MSG " + reply + b" 1 6\r\nintact\r\n")
                    else:
                        assert len(f.read(size + 2)) == size + 2
                        if mode == "error" and not sent_error:
                            sent_error = True
                            s.sendall(b'-ERR \'Permissions Violation for Publish to "denied"\'\r\n' * 40)
                        elif mode == "status0":
                            hdr = b"NATS/1.0 503\r\n\r\n"
                            n = str(len(hdr)).encode()
                            s.sendall(b"HMSG t 1 " + n + b" " + n + b"\r\n" + hdr + b"\r\n")
            f.close()
        except Exception as exc:
            errors.append(exc)
        finally:
            for s in clients:
                s.close()
            listener.close()

    thread = threading.Thread(target=server, daemon=True)
    thread.start()
    proc = subprocess.run([BIN, mode, f"nats://127.0.0.1:{port}"],
                          text=True, capture_output=True, timeout=6)
    thread.join(5)
    assert proc.returncode == 0, f"{mode}: {proc.stdout}{proc.stderr}"
    assert not thread.is_alive(), f"{mode}: server did not finish"
    assert not errors, f"{mode}: peer errors: {errors}"
    print(f"OK: fault peer {mode}", flush=True)


for scenario in sys.argv[1:] or ["failed-dial", "auth", "error", "deadline",
                                "status0", "write", "partial", "ping", "malformed"]:
    run(scenario)
