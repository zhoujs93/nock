#!/usr/bin/env python3
import argparse, json, os, re, sys, time, threading, subprocess
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, HTTPServer
from collections import deque

# ----------- regex patterns -----------
RE_PPS_1 = re.compile(r'(?i)\b([0-9]+(?:\.[0-9]+)?)\s*(?:p/s|proofs?/s)\b')
RE_PPS_2 = re.compile(r'(?i)\b(?:proof\s*rate|proof_rate)\b[^0-9]*([0-9]+(?:\.[0-9]+)?)')
RE_KPM   = re.compile(r'(?i)\b([0-9]+(?:\.[0-9]+)?)\s*k[p]?/m\b')  # Kp/m → p/s

def ts():
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S%z")

# ----------- sliding window tracker -----------
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
            vals = [v for _, v in self.samples if now - _ <= self.window]
            med = (sorted(vals)[len(vals)//2] if vals else None)
            age = now - self.last_line_at if self.last_line_at else None
            return {"median_pps": med, "n": len(vals), "age_sec": age}

# ----------- tailers -----------
def tail_journal(unit: str, cb):
    # follow new lines from "now"
    cmd = ["journalctl", "-u", unit, "-f", "-o", "cat", "--since", "now"]
    with subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, bufsize=1) as p:
        for line in p.stdout:
            cb(line.rstrip("\n"))

def tail_file(path: str, cb):
    with open(path, "r", errors="replace") as f:
        f.seek(0, os.SEEK_END)
        while True:
            line = f.readline()
            if not line:
                time.sleep(0.1); continue
            cb(line.rstrip("\n"))

# ----------- GPU sampler -----------
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

# ----------- HTTP server -----------
class Metrics:
    def __init__(self):
        self.lock = threading.Lock()
        self.data = {
            "time": ts(),
            "proof_rate": {"median_pps": None, "n": 0, "age_sec": None},
            "gpus": [],    # list of dicts from sample_gpu()
            "meta": {},    # user-supplied tags
        }

    def update(self, pr_snapshot=None, gpus=None, meta=None):
        with self.lock:
            if pr_snapshot is not None:
                self.data["proof_rate"] = pr_snapshot
            if gpus is not None:
                self.data["gpus"] = gpus
            if meta:
                self.data["meta"].update(meta)
            self.data["time"] = ts()

    def get(self):
        with self.lock:
            return dict(self.data)

def run_http(metrics: Metrics, host: str, port: int, token: str):
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            # simple token check in header or query ?token=
            ok = False
            if token:
                auth = self.headers.get("X-Token")
                if auth == token:
                    ok = True
                else:
                    # parse query
                    if "?" in self.path:
                        try:
                            q = dict(x.split("=",1) for x in self.path.split("?",1)[1].split("&"))
                            ok = (q.get("token") == token)
                        except Exception:
                            ok = False
            else:
                ok = True

            if not ok:
                self.send_response(401); self.end_headers(); return

            if self.path.startswith("/metrics.json") or self.path.startswith("/"):
                body = json.dumps(metrics.get()).encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type","application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
            else:
                self.send_response(404); self.end_headers()

        def log_message(self, fmt, *args):  # silence access log
            return

    httpd = HTTPServer((host, port), Handler)
    httpd.serve_forever()

def main():
    ap = argparse.ArgumentParser(description="Nock miner exporter (proof rate + GPU util → HTTP JSON)")
    src = ap.add_mutually_exclusive_group(required=True)
    src.add_argument("--journal-unit", help="systemd unit to follow (e.g. nockpool-miner.service)")
    src.add_argument("--log-file", help="file to tail")
    ap.add_argument("--host", default="0.0.0.0")
    ap.add_argument("--port", type=int, default=9108)
    ap.add_argument("--token", default=os.environ.get("NOCK_EXPORTER_TOKEN",""))
    ap.add_argument("--window-sec", type=int, default=120)
    ap.add_argument("--gpu-sample-sec", type=int, default=15)
    ap.add_argument("--tag", default=os.environ.get("NOCK_TAG",""), help="free-form label (e.g. label/instance id)")
    args = ap.parse_args()

    tracker = RateTracker(window_sec=args.window_sec)
    metrics = Metrics()
    if args.tag:
        metrics.update(meta={"tag": args.tag})

    # tail thread
    t_tail = threading.Thread(
        target=tail_journal if args.journal_unit else tail_file,
        args=((args.journal_unit or args.log_file), tracker.observe),
        daemon=True)
    t_tail.start()

    # GPU sampler loop
    def gpu_loop():
        while True:
            g = sample_gpu()
            metrics.update(pr_snapshot=tracker.snapshot(), gpus=g)
            time.sleep(args.gpu_sample_sec)
    threading.Thread(target=gpu_loop, daemon=True).start()

    # HTTP server
    run_http(metrics, args.host, args.port, args.token)

if __name__ == "__main__":
    main()
