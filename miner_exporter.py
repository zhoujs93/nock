#!/usr/bin/env python3
"""
Push-only Nock miner exporter.
- tails miner log (or a systemd unit)
- computes median proofs/sec over a sliding window
- samples GPU util/power via nvidia-smi
- periodically POSTs JSON to a central collector

Default collector: http://78.46.165.58:9000/ingest
Auth header: X-Token (optional)
"""

import argparse, json, os, re, sys, time, threading, subprocess
from datetime import datetime, timezone
from collections import deque

# ---------------- HTTP client (stdlib only) ----------------
import urllib.request, urllib.error

def http_post(url: str, headers: dict, payload: dict, timeout: float = 3.0) -> int:
    data = json.dumps(payload).encode("utf-8")
    hdrs = {"Content-Type": "application/json"}
    if headers:
        hdrs.update(headers)
    req = urllib.request.Request(url, data=data, headers=hdrs, method="POST")
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return resp.getcode()

# ---------------- Utilities ----------------
RE_PPS_1 = re.compile(r'(?i)\b([0-9]+(?:\.[0-9]+)?)\s*(?:p/s|proofs?/s)\b')
RE_PPS_2 = re.compile(r'(?i)\b(?:proof\s*rate|proof_rate)\b[^0-9]*([0-9]+(?:\.[0-9]+)?)')
RE_KPM   = re.compile(r'(?i)\b([0-9]+(?:\.[0-9]+)?)\s*k[p]?/m\b')   # Kp/m → p/s

def ts():
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S%z")

class RateTracker:
    def __init__(self, window_sec=120):
        self.window = window_sec
        self.lock = threading.Lock()
        self.samples = deque()  # (t, pps)
        self.last_line_at = 0.0

    def observe(self, line: str):
        pps = None
        m = RE_PPS_1.search(line) or RE_PPS_2.search(line)
        if m:
            pps = float(m.group(1))
        else:
            m = RE_KPM.search(line)
            if m:
                pps = float(m.group(1)) * 1000.0 / 60.0
        if pps is not None:
            now = time.time()
            with self.lock:
                self.samples.append((now, pps))
                self.last_line_at = now
                cutoff = now - self.window
                while self.samples and self.samples[0][0] < cutoff:
                    self.samples.popleft()

    def snapshot(self):
        with self.lock:
            now = time.time()
            vals = [v for t, v in self.samples if now - t <= self.window]
            med = (sorted(vals)[len(vals)//2] if vals else None)
            age = now - self.last_line_at if self.last_line_at else None
            return {"median_pps": med, "n": len(vals), "age_sec": age}

def tail_journal(unit: str, cb):
    cmd = ["journalctl", "-u", unit, "-f", "-o", "cat", "--since", "now"]
    with subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, bufsize=1) as p:
        for line in p.stdout:
            cb(line.rstrip("\n"))

def tail_file(path: str, cb):
    # wait for file to appear if miner starts slightly later
    while not os.path.exists(path):
        time.sleep(0.5)
    with open(path, "r", errors="replace") as f:
        f.seek(0, os.SEEK_END)
        while True:
            line = f.readline()
            if not line:
                time.sleep(0.1); continue
            cb(line.rstrip("\n"))

def sample_gpu():
    try:
        out = subprocess.check_output(
            ["nvidia-smi",
             "--query-gpu=name,index,utilization.gpu,utilization.memory,power.draw,temperature.gpu",
             "--format=csv,noheader,nounits"],
            text=True, timeout=2.5).strip().splitlines()
        gpus = []
        for row in out:
            parts = [p.strip() for p in row.split(",")]
            if len(parts) >= 6:
                gpus.append({
                    "name": parts[0],
                    "index": int(parts[1]),
                    "gpu_util": float(parts[2]),
                    "mem_util": float(parts[3]),
                    "power_w": float(parts[4]),
                    "temp_c": float(parts[5]),
                })
        return gpus
    except Exception:
        return []

# ---------------- Main ----------------
def main():
    ap = argparse.ArgumentParser(description="Nock miner exporter (push-only)")
    src = ap.add_mutually_exclusive_group(required=True)
    src.add_argument("--journal-unit", help="systemd unit to follow (e.g. nockpool-miner.service)")
    src.add_argument("--log-file", help="file to tail, e.g. /var/log/nockminer.log")

    # proof-rate window & GPU sampling cadence
    ap.add_argument("--window-sec", type=int, default=int(os.environ.get("WINDOW_SEC","120")))
    ap.add_argument("--gpu-sample-sec", type=int, default=int(os.environ.get("GPU_SAMPLE_SEC","15")))

    # push settings
    ap.add_argument("--post-url", default=os.environ.get("MONITOR_URL","http://78.46.165.58:9000/ingest"),
                    help="Collector endpoint to POST metrics JSON")
    ap.add_argument("--post-token", default=os.environ.get("MONITOR_TOKEN",""),
                    help="Shared secret sent as X-Token")
    ap.add_argument("--post-interval", type=int, default=int(os.environ.get("POST_INTERVAL","30")),
                    help="Seconds between pushes")

    # identity / tagging
    ap.add_argument("--node-id", default=os.environ.get("NODE_ID", os.environ.get("HOSTNAME","")),
                    help="identifier for this miner (hostname/label/contract id)")
    ap.add_argument("--tag", default=os.environ.get("NOCK_TAG",""),
                    help="free-form label to include in meta")

    args = ap.parse_args()

    tracker = RateTracker(window_sec=args.window_sec)

    # tail thread
    t_tail = threading.Thread(
        target=tail_journal if args.journal_unit else tail_file,
        args=((args.journal_unit or args.log_file), tracker.observe),
        daemon=True)
    t_tail.start()

    # background sampler + push loop
    headers = {}
    if args.post_token:
        headers["X-Token"] = args.post_token

    ext_port = os.environ.get("VAST_TCP_PORT_9108")  # for debugging only; not required in push-mode

    # lightweight state to include GPU stats at each push
    def loop():
        backoff = 1.0
        while True:
            try:
                payload = {
                    "time": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S%z"),
                    "proof_rate": tracker.snapshot(),
                    "gpus": sample_gpu(),
                    "meta": {
                        "node_id": args.node_id,
                        "tag": args.tag,
                        "source": "push-exporter",
                        "exporter_ext_port": ext_port
                    }
                }
                http_post(args.post_url, headers, payload, timeout=3.0)
                backoff = 1.0  # reset on success
            except Exception:
                # exponential backoff on error, capped at 60s
                backoff = min(backoff * 2.0, 60.0)
            time.sleep(max(args.post_interval, int(backoff)))

    threading.Thread(target=loop, daemon=True).start()

    # keep the process alive
    while True:
        time.sleep(3600)

if __name__ == "__main__":
    main()
