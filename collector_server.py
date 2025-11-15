#!/usr/bin/env python3
import argparse, json, time
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, HTTPServer
from threading import Lock

def ts():
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S%z")

class Store:
    def __init__(self):
        self.lock = Lock()
        self.by_node = {}  # node_id -> last payload
    def update(self, node_id, payload):
        with self.lock:
            self.by_node[node_id] = {"t": time.time(), "payload": payload}
    def snapshot(self):
        with self.lock:
            return dict(self.by_node)

def run(host, port, token):
    store = Store()
    class H(BaseHTTPRequestHandler):
        def do_POST(self):
            if self.path != "/ingest":
                self.send_response(404); self.end_headers(); return
            if token:
                if self.headers.get("X-Token") != token:
                    self.send_response(401); self.end_headers(); return
            ln = int(self.headers.get("Content-Length","0"))
            try:
                payload = json.loads(self.rfile.read(ln) or b"{}")
            except Exception:
                self.send_response(400); self.end_headers(); return
            meta = payload.get("meta") or {}
            node_id = meta.get("node_id") or self.client_address[0]
            store.update(node_id, payload)
            self.send_response(204); self.end_headers()
        def do_GET(self):
            # human-readable summary
            if self.path.startswith("/"):
                snap = store.snapshot()
                rows = []
                for node, rec in snap.items():
                    p = rec["payload"]
                    pr = (p.get("proof_rate") or {})
                    g  = (p.get("gpus") or [])
                    util = round(sum(d.get("gpu_util",0.0) for d in g)/len(g),1) if g else None
                    rows.append({
                        "node": node,
                        "pps": pr.get("median_pps"),
                        "age": pr.get("age_sec"),
                        "util": util,
                        "tag": (p.get("meta") or {}).get("tag","")
                    })
                body = (json.dumps({"time": ts(), "miners": rows}, indent=2)).encode()
                self.send_response(200)
                self.send_header("Content-Type","application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers(); self.wfile.write(body)
    HTTPServer((host, port), H).serve_forever()

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", default="0.0.0.0")
    ap.add_argument("--port", type=int, default=9100)
    ap.add_argument("--token", default="putsncalls23")
    args = ap.parse_args()
    print(f"[{ts()}] collector listening on {args.host}:{args.port}")
    run(args.host, args.port, args.token)
