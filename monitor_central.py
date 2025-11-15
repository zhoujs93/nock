#!/usr/bin/env python3
import argparse, time, json, requests, concurrent.futures
from datetime import datetime, timezone

API_BASE = "https://console.vast.ai/api/v0"

def ts():
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S%z")

def discover_targets(api_key: str, label_prefix: str, port: int):
    hdr = {"Authorization": f"Bearer {api_key}"}
    r = requests.get(f"{API_BASE}/instances/", headers=hdr, timeout=20)
    r.raise_for_status()
    out = []
    for ins in (r.json().get("instances") or []):
        lbl = (ins.get("label") or "")
        st  = (ins.get("cur_state") or ins.get("actual_status") or "").lower()
        if not lbl.startswith(label_prefix): continue
        if st != "running": continue
        ip = ins.get("public_ipaddr") or ins.get("ip") or ins.get("host_public_ip")
        if ip:
            out.append(f"{ip}:{port}")
    return out

def fetch_one(target: str, token: str, timeout=3.0):
    url = f"http://{target}/metrics.json"
    hdr = {"X-Token": token} if token else {}
    try:
        r = requests.get(url, headers=hdr, timeout=timeout)
        if r.ok:
            data = r.json()
            pr = data.get("proof_rate", {})
            g  = data.get("gpus", [])
            avg_util = round(sum(x.get("gpu_util",0.0) for x in g)/len(g),1) if g else None
            return {
                "target": target,
                "pps": pr.get("median_pps"),
                "n": pr.get("n"),
                "age": pr.get("age_sec"),
                "gpu_util": avg_util,
                "tag": (data.get("meta") or {}).get("tag","")
            }
        else:
            return {"target": target, "error": f"HTTP {r.status_code}"}
    except Exception as e:
        return {"target": target, "error": str(e)}

def main():
    ap = argparse.ArgumentParser(description="Central Nock proof-rate monitor")
    ap.add_argument("--targets", help="Comma-separated list of ip:port")
    ap.add_argument("--discover_vast", action="store_true")
    ap.add_argument("--api-key", default="")
    ap.add_argument("--label-prefix", default="deal-snipe")
    ap.add_argument("--exporter-port", type=int, default=9108)
    ap.add_argument("--token", default="")
    ap.add_argument("--interval", type=int, default=15)
    ap.add_argument("--workers", type=int, default=32)
    args = ap.parse_args()

    targets = []
    if args.targets:
        targets = [t.strip() for t in args.targets.split(",") if t.strip()]
    if args.discover_vast:
        if not args.api_key:
            print("ERROR: --api-key required with --discover-vast"); return
        targets = discover_targets(args.api_key, args.label_prefix, args.exporter_port)

    if not targets:
        print("No targets to query."); return

    while True:
        with concurrent.futures.ThreadPoolExecutor(max_workers=args.workers) as ex:
            futs = [ex.submit(fetch_one, t, args.token) for t in targets]
            rows = [f.result() for f in futs]

        # sort by pps desc (None last)
        rows.sort(key=lambda r: (-(r["pps"] or -1e9)))
        print(f"\n[{ts()}] {len(rows)} miners")
        print(f"{'target':22s}  {'pps':>7s}  {'util%':>6s}  {'n':>3s}  {'age(s)':>6s}  tag")
        for r in rows:
            if "error" in r:
                print(f"{r['target']:22s}  {'ERR':>7s}  {'':6s}  {'':3s}  {'':6s}  {r.get('error','')}")
            else:
                pps = f"{r['pps']:.3f}" if r["pps"] is not None else "-"
                util = f"{r['gpu_util']:.1f}" if r["gpu_util"] is not None else "-"
                age  = f"{int(r['age']):d}" if r["age"] is not None else "-"
                print(f"{r['target']:22s}  {pps:>7s}  {util:>6s}  {str(r['n']):>3s}  {age:>6s}  {r['tag']}")
        time.sleep(args.interval)

if __name__ == "__main__":
    main()
