#!/usr/bin/env python3
import argparse, json, time, logging, os, tempfile
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, HTTPServer
from threading import Lock, Thread
from urllib.parse import urlparse, parse_qs

# Optional YAML support
try:
    import yaml
    _YAML_AVAILABLE = True
except Exception:
    yaml = None
    _YAML_AVAILABLE = False

# -------- logging --------
def ts():
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S%z")

def setup_logging(verbose: bool):
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S%z",
    )

# -------- store --------
class Store:
    def __init__(self):
        self.lock = Lock()
        self.by_node = {}  # node_id -> {t, payload}

    def update(self, node_id, payload):
        with self.lock:
            self.by_node[node_id] = {"t": time.time(), "payload": payload}

    def snapshot(self):
        with self.lock:
            return dict(self.by_node)

    # ---- NEW: YAML dump helper ----
    def dump_yaml(self, path: str,
                  *,
                  redact_keys=("X-Token", "Authorization", "authorization",
                               "jupyter_token", "jupyterToken", "token", "secret", "password"),
                  include_age: bool = True) -> str:
        """
        Atomically write the current snapshot to YAML at `path` (JSON if PyYAML not installed).
        Returns the absolute path written.
        """
        snap = self.snapshot()  # shallow copy
        now = time.time()

        def _redact(obj):
            if isinstance(obj, dict):
                out = {}
                for k, v in obj.items():
                    if k in redact_keys:
                        out[k] = "***REDACTED***"
                    else:
                        out[k] = _redact(v)
                return out
            elif isinstance(obj, list):
                return [_redact(x) for x in obj]
            return obj

        # Build a human-friendly structure
        rows = []
        for node, rec in snap.items():
            p = rec.get("payload") or {}
            backend = p.get("backend")
            cpu_disabled = p.get("cpu_disabled")
            if backend is None:
                if cpu_disabled is True:
                    backend = "cpu"
                elif cpu_disabled is False:
                    backend = "gpu"
            entry = {
                "node": node,
                "backend": backend,
                "cpu_disabled": cpu_disabled,
                "backend_age": p.get("cpu_disabled_age_sec") or p.get("backend_age_sec"),
                "last_seen_sec": round(now - rec.get("t", now), 1),
                "payload": _redact(p),
            }
            rows.append(entry)

        doc = {
            "dump_time": ts(),
            "count": len(rows),
            "miners": rows
        }

        # Serialize
        text = None
        if _YAML_AVAILABLE:
            # default_flow_style=False -> block style; sort_keys=False for readability
            text = yaml.safe_dump(doc, sort_keys=False)
        else:
            text = json.dumps(doc, indent=2)

        # Atomic write
        abs_path = os.path.abspath(path)
        os.makedirs(os.path.dirname(abs_path), exist_ok=True)
        fd, tmp = tempfile.mkstemp(prefix=".collector_dump_", dir=os.path.dirname(abs_path))
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                f.write(text)
            os.replace(tmp, abs_path)
        finally:
            try:
                if os.path.exists(tmp):
                    os.remove(tmp)
            except Exception:
                pass

        logging.info("dumped snapshot to %s (%s format)", abs_path,
                     "YAML" if _YAML_AVAILABLE else "JSON")
        return abs_path

# -------- server --------
def run(host, port, token, verbose=False,
        debug_payload=False,
        dump_path: str | None = None,
        dump_interval: int = 0,
        dump_on_ingest: bool = False):
    store = Store()

    # Optional periodic dumper
    def _periodic_dump():
        while True:
            try:
                time.sleep(dump_interval)
            except Exception:
                break
            try:
                if dump_path:
                    store.dump_yaml(dump_path)
            except Exception as e:
                logging.warning("periodic dump failed: %s", e)

    if dump_path and dump_interval and dump_interval > 0:
        Thread(target=_periodic_dump, daemon=True).start()

    class H(BaseHTTPRequestHandler):
        _token = token
        _debug_payload = debug_payload

        def _log_ingest_summary(self, ip: str, node_id: str, payload: dict, status: int):
            backend = payload.get("backend")
            cpu_disabled = payload.get("cpu_disabled")
            if backend is None:
                if cpu_disabled is True:
                    backend = "cpu"
                elif cpu_disabled is False:
                    backend = "gpu"
            age = payload.get("cpu_disabled_age_sec") or payload.get("backend_age_sec") or None
            age_s = f"{age:.1f}s" if isinstance(age, (int, float)) else "n/a"

            pr = payload.get("proof_rate") or {}
            pps = pr.get("median_pps")
            g  = payload.get("gpus") or []
            util = round(sum(d.get("gpu_util", 0.0) for d in g) / len(g), 1) if g else None
            tag = (payload.get("meta") or {}).get("tag", "")

            logging.info(
                "ingest ip=%s status=%s node=%s backend=%s cpu_disabled=%s age=%s pps=%s util=%s tag=%s",
                ip, status, node_id, backend, cpu_disabled, age_s, pps, util, tag
            )
            if self._debug_payload:
                try:
                    logging.debug("payload=%s", json.dumps(payload, separators=(",", ":")))
                except Exception:
                    logging.debug("payload=(unprintable)")

        def do_POST(self):
            if self.path != "/ingest":
                self.send_response(404); self.end_headers(); return

            if self._token:
                if self.headers.get("X-Token") != self._token:
                    self.send_response(401); self.end_headers()
                    logging.warning("unauthorized POST from %s", self.client_address[0])
                    return

            ln = int(self.headers.get("Content-Length", "0"))
            try:
                raw = self.rfile.read(ln) if ln > 0 else b"{}"
                payload = json.loads(raw or b"{}")
            except Exception:
                self.send_response(400); self.end_headers()
                logging.warning("bad JSON from %s", self.client_address[0])
                return

            meta = payload.get("meta") or {}
            node_id = meta.get("node_id") or self.client_address[0]
            store.update(node_id, payload)

            self.send_response(204)
            self.end_headers()
            self._log_ingest_summary(self.client_address[0], node_id, payload, status=204)

            # Optional dump on ingest
            if dump_on_ingest and dump_path:
                try:
                    store.dump_yaml(dump_path)
                except Exception as e:
                    logging.warning("dump_on_ingest failed: %s", e)

        def do_GET(self):
            url = urlparse(self.path)
            if url.path == "/dump":
                # On-demand YAML/JSON response; optionally write to disk if "?write=1"
                try_write = (parse_qs(url.query).get("write", ["0"])[0] == "1")
                snap = store.snapshot()
                now = time.time()

                # Build same structure as dump_yaml (without redaction)
                rows = []
                for node, rec in snap.items():
                    p = rec.get("payload") or {}
                    backend = p.get("backend")
                    cpu_disabled = p.get("cpu_disabled")
                    if backend is None:
                        if cpu_disabled is True:
                            backend = "cpu"
                        elif cpu_disabled is False:
                            backend = "gpu"
                    rows.append({
                        "node": node,
                        "backend": backend,
                        "cpu_disabled": cpu_disabled,
                        "backend_age": p.get("cpu_disabled_age_sec") or p.get("backend_age_sec"),
                        "last_seen_sec": round(now - rec.get("t", now), 1),
                        "payload": p,
                    })
                doc = {"dump_time": ts(), "count": len(rows), "miners": rows}

                if try_write and dump_path:
                    try:
                        store.dump_yaml(dump_path)
                    except Exception as e:
                        logging.warning("GET /dump write failed: %s", e)

                body: bytes
                if _YAML_AVAILABLE:
                    body = yaml.safe_dump(doc, sort_keys=False).encode()
                    ctype = "text/yaml"
                else:
                    body = json.dumps(doc, indent=2).encode()
                    ctype = "application/json"

                self.send_response(200)
                self.send_header("Content-Type", ctype)
                self.send_header("Content-Length", str(len(body)))
                self.end_headers(); self.wfile.write(body)
                return

            # Default root summary
            if url.path.startswith("/"):
                snap = store.snapshot()
                rows = []
                now = time.time()
                for node, rec in snap.items():
                    p = rec["payload"] or {}
                    backend = p.get("backend")
                    cpu_disabled = p.get("cpu_disabled")
                    if backend is None:
                        if cpu_disabled is True:
                            backend = "cpu"
                        elif cpu_disabled is False:
                            backend = "gpu"
                    rows.append({
                        "node": node,
                        "backend": backend,
                        "cpu_disabled": cpu_disabled,
                        "backend_age": p.get("cpu_disabled_age_sec") or p.get("backend_age_sec"),
                        "last_seen_sec": round(now - rec["t"], 1),
                        "tag": (p.get("meta") or {}).get("tag","")
                    })
                body = (json.dumps({"time": ts(), "miners": rows}, indent=2)).encode()
                self.send_response(200)
                self.send_header("Content-Type","application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers(); self.wfile.write(body)

        def log_message(self, fmt, *args):
            logging.info("%s - %s", self.client_address[0], fmt % args)

    logging.info("collector listening on %s:%s", host, port)
    HTTPServer((host, port), H).serve_forever()

# -------- main --------
if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", default="0.0.0.0")
    ap.add_argument("--port", type=int, default=9100)
    ap.add_argument("--token", default="putsncalls23", help="X-Token required for POST /ingest")
    ap.add_argument("--verbose", action="store_true", help="INFO/DEBUG logging")
    ap.add_argument("--debug-payload", action="store_true", help="Log full JSON payloads at DEBUG")

    # YAML dumping controls
    ap.add_argument("--dump-path", default="./data/collector_dump.yaml", help="Path to write YAML/JSON snapshot")
    ap.add_argument("--dump-interval", type=int, default=60, help="Seconds between periodic dumps (0=off)")
    ap.add_argument("--dump-on-ingest", action="store_true", help="Also write a dump after each /ingest")

    args = ap.parse_args()
    setup_logging(args.verbose)
    logging.info("[%s] starting collector", ts())

    run(
        args.host, args.port, args.token,
        verbose=args.verbose,
        debug_payload=args.debug_payload,
        dump_path=(args.dump_path or None),
        dump_interval=args.dump_interval,
        dump_on_ingest=args.dump_on_ingest
    )
