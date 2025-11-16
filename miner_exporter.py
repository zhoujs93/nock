#!/usr/bin/env python3
"""
Push-only Nock miner exporter (CPU-fallback detection only).

- Tails the miner's stdout/log (journal unit or a file)
- Detects the line: "GPU mining disabled, using CPU mining" (ANSI stripped)
- Periodically POSTs a minimal JSON to a central collector:
    {
      "time": "...",
      "cpu_disabled": true|false|null,
      "cpu_disabled_age_sec": <seconds or null>,
      "backend": "cpu"|"gpu"|null,   # convenience field ("cpu" if cpu_disabled==True, "gpu" if False)
      "meta": { "node_id": "...", "tag": "...", "source": "push-exporter" }
    }

Defaults:
  POST URL: http://78.46.165.58:9000/ingest
  Auth header: X-Token (optional)

Notes:
- We do NOT parse proof rates anymore.
- We do NOT require any Vast port mappings (push model).
"""

import argparse, json, os, re, sys, time, threading, subprocess
from datetime import datetime, timezone
from http.client import HTTPException
import urllib.request, urllib.error

# ---------- HTTP ----------
def http_post(url: str, headers: dict, payload: dict, timeout: float = 3.0) -> int:
    data = json.dumps(payload).encode("utf-8")
    hdrs = {"Content-Type": "application/json"}
    if headers:
        hdrs.update(headers)
    req = urllib.request.Request(url, data=data, headers=hdrs, method="POST")
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return resp.getcode()

# ---------- Utils ----------
ANSI_RE = re.compile(r'\x1b\[[0-9;]*[A-Za-z]')
RE_CPU_DISABLED = re.compile(r'GPU\s+mining\s+disabled,\s+using\s+CPU\s+mining', re.I)
RE_CPU_THREADS  = re.compile(r'\bmining\s+with\s+\d+\s+threads\b', re.I)  # optional corroboration

def ts() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S%z")

# ---------- Tailers ----------
def tail_journal(unit: str, cb):
    # journalctl -f from "now"
    cmd = ["journalctl", "-u", unit, "-f", "-o", "cat", "--since", "now"]
    with subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, bufsize=1) as p:
        for line in p.stdout:
            cb(line.rstrip("\n"))

def tail_file(path: str, cb):
    # wait until file exists (miner might start after exporter)
    while not os.path.exists(path):
        time.sleep(0.5)
    with open(path, "r", errors="replace") as f:
        f.seek(0, os.SEEK_END)
        while True:
            line = f.readline()
            if not line:
                time.sleep(0.1); continue
            cb(line.rstrip("\n"))

# ---------- Minimal state ----------
class CpuFallbackTracker:
    """
    Tracks whether we've seen the explicit CPU-fallback line.
    Once seen, 'cpu_disabled' stays True for this exporter lifetime.
    On container restart (exporter restart), state resets to unknown.
    """
    def __init__(self):
        self.cpu_disabled = None     # None=unknown, True=CPU-only, False=not observed
        self.cpu_disabled_at = 0.0
        self.last_line_at = 0.0
        self.lock = threading.Lock()

    def observe(self, line: str):
        clean = ANSI_RE.sub('', line)
        now = time.time()
        with self.lock:
            self.last_line_at = now
            if self.cpu_disabled is not True:
                if RE_CPU_DISABLED.search(clean) or RE_CPU_THREADS.search(clean):
                    self.cpu_disabled = True
                    self.cpu_disabled_at = now
            # If you WANT to mark an explicit "gpu" mode, you could add a positive signal here.
            # Per your request, we do not; absence of the CPU line => treat as False at push time.

    def snapshot(self) -> dict:
        with self.lock:
            # If we haven't seen the CPU line yet, report False (i.e., assume GPU path)
            # If you prefer strict "unknown" until a decision, set to None instead.
            cpu = self.cpu_disabled if self.cpu_disabled is not None else False
            age = (time.time() - self.cpu_disabled_at) if (self.cpu_disabled_at > 0) else None
            backend = ("cpu" if cpu else "gpu") if cpu is not None else None
            return {
                "cpu_disabled": cpu,
                "cpu_disabled_age_sec": age,
                "backend": backend
            }

# ---------- Main ----------
def main():
    ap = argparse.ArgumentParser(description="Nock miner exporter (CPU-fallback only, push-mode)")
    src = ap.add_mutually_exclusive_group(required=True)
    src.add_argument("--journal-unit", help="systemd unit to follow (e.g. nockpool-miner.service)")
    src.add_argument("--log-file", help="file to tail, e.g. /var/log/nockminer.log")

    # push settings
    ap.add_argument("--post-url", default=os.environ.get("MONITOR_URL","http://78.46.165.58:9100/ingest"),
                    help="Collector endpoint to POST JSON")
    ap.add_argument("--post-token", default=os.environ.get("MONITOR_TOKEN",""),
                    help="Shared secret sent as X-Token")
    ap.add_argument("--post-interval", type=int, default=int(os.environ.get("POST_INTERVAL","3600")),
                    help="Seconds between pushes (default 30)")

    # identity / tagging
    default_node = os.environ.get("NODE_ID") or os.environ.get("HOSTNAME") or ""
    ap.add_argument("--node-id", default=default_node,
                    help="identifier for this miner (hostname/label/contract id)")
    ap.add_argument("--tag", default=os.environ.get("NOCK_TAG",""),
                    help="free-form label to include in meta")

    args = ap.parse_args()

    tracker = CpuFallbackTracker()

    # tail thread
    tail_target = args.journal_unit if args.journal_unit else args.log_file
    t_tail = threading.Thread(
        target=tail_journal if args.journal_unit else tail_file,
        args=(tail_target, tracker.observe),
        daemon=True)
    t_tail.start()

    # push loop
    headers = {}
    if args.post_token:
        headers["X-Token"] = args.post_token

    def loop():
        backoff = 1.0
        while True:
            snap = tracker.snapshot()
            payload = {
                "time": ts(),
                "cpu_disabled": snap["cpu_disabled"],
                "cpu_disabled_age_sec": snap["cpu_disabled_age_sec"],
                "backend": snap["backend"],
                "meta": {
                    "node_id": args.node_id,
                    "tag": args.tag,
                    "source": "push-exporter"
                }
            }
            try:
                http_post(args.post_url, headers, payload, timeout=3.0)
                backoff = 1.0
            except Exception:
                backoff = min(backoff * 2.0, 60.0)  # gentle backoff on collector hiccups
            time.sleep(max(3, int(args.post_interval)))  # don’t spam the collector

    threading.Thread(target=loop, daemon=True).start()

    # keep process alive
    try:
        while True:
            time.sleep(3600)
    except KeyboardInterrupt:
        pass

if __name__ == "__main__":
    main()
