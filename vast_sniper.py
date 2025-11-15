import os, sys, time, json, argparse
from datetime import datetime, timezone
from typing import List, Dict, Optional, Set, Tuple
from dataclasses import dataclass, field

import requests
import duckdb
import pandas as pd
import asyncio, aiohttp
from collections import defaultdict
from dotenv import load_dotenv, find_dotenv

# ------------------------------- GPU model sets (space-form) ------------------
load_dotenv(find_dotenv(), override=False)

API_KEY = os.getenv("API_KEY")
API_SECRET = os.getenv("TEMPLATE_HASH")

_last_rebid_at = defaultdict(lambda: 0.0)  # instance_id -> epoch seconds

# ------------- Models -------------
GPU_30_SERIES = [
    "RTX 3080", "RTX 3080 Ti",
    "RTX 3090", "RTX 3090 Ti",
]
GPU_40_SERIES = ["RTX 4070","RTX 4070 Ti","RTX 4070 Super","RTX 4070 Ti Super","RTX 4080","RTX 4080 Super","RTX 4090","RTX 4090D"]
API_BASE = "https://console.vast.ai/api/v0"

def ts() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S%z")

def country_of(geo: str) -> str:
    try: return geo.rsplit(",", 1)[-1].strip().upper()
    except Exception: return ""

def round_up(x: float, places: int = 6) -> float:
    scale = 10 ** places
    return (int(x * scale + 0.999999) / scale)

# -------- Config & Parser --------
@dataclass
class Config:
    api_key: str
    template_hash_id: Optional[str] = None
    template_id: Optional[int] = None

    gpu_models: List[str] = field(default_factory=lambda: sorted(set(GPU_30_SERIES + GPU_40_SERIES)))
    countries: List[str] = field(default_factory=lambda: ["US","CA"])
    min_reliability: float = 0.95
    instance_type: str = "bid"        # for new rentals
    limit: int = 200
    poll_sec: int = 30
    window_min: int = 120
    discount: float = 0.50            # 50% off baseline
    max_dph: float = 0.0              # 0=off
    print_top: int = 8

    auto_rent: bool = True
    exit_after_rent: bool = False
    max_instances: int = 10
    active_states: Set[str] = field(default_factory=lambda: {"running","starting","restarting"})
    label_prefix: str = "deal-snipe"

    # rebid ladder
    rebid_on_preempt: bool = True
    rebid_steps: List[float] = field(default_factory=lambda: [1.00, 1.02, 1.05])
    rebid_max_frac_od: float = 0.90   # cap at 90% of on-demand median
    rebid_interval_sec: int = 60
    warmup_sec: int = 120             # boot+miner start for break-even

    db_path: str = "./data/vast_prices.duckdb"

    min_gpus_per_offer: int = 1
    max_gpus_per_offer: Optional[int] = 4

def parse_args_to_config() -> Config:
    # allow env fallbacks
    api_key = os.getenv("API_KEY")
    if not api_key:
        print("ERROR: Set API_KEY or VAST_API_KEY", file=sys.stderr); sys.exit(1)
    template_hash = os.getenv("TEMPLATE_HASH") or os.getenv("API_SECRET") or ""

    p = argparse.ArgumentParser("VastSniper (class refactor)")
    p.add_argument("--countries", default=os.getenv("COUNTRIES","US,CA"))
    p.add_argument("--min-reliability", type=float, default=float(os.getenv("MIN_RELIABILITY","0.95")))
    p.add_argument("--instance-type", choices=["bid","on-demand","reserved"], default=os.getenv("INSTANCE_TYPE","bid"))
    p.add_argument("--limit", type=int, default=int(os.getenv("LIMIT","200")))
    p.add_argument("--poll-sec", type=int, default=int(os.getenv("POLL_SEC","30")))
    p.add_argument("--window-min", type=int, default=int(os.getenv("WINDOW_MIN","120")))
    p.add_argument("--discount", type=float, default=float(os.getenv("DISCOUNT","0.25")))
    p.add_argument("--max-dph", type=float, default=float(os.getenv("MAX_DPH","0.0")))
    p.add_argument("--print-top", type=int, default=int(os.getenv("PRINT_TOP","8")))
    p.add_argument("--auto-rent", action="store_true", default=os.getenv("AUTO_RENT","1").lower() in {"1","true","yes"})
    p.add_argument("--exit-after-rent", action="store_true", default=os.getenv("EXIT_AFTER_RENT","0").lower() in {"1","true","yes"})
    p.add_argument("--max-instances", type=int, default=int(os.getenv("MAX_INSTANCES","50")))
    p.add_argument("--active-states", default=os.getenv("ACTIVE_STATES","running,starting,restarting"))
    p.add_argument("--label-prefix", default=os.getenv("LABEL_PREFIX","deal-snipe"))
    p.add_argument("--template-id", type=int, default=int(os.getenv("TEMPLATE_ID","0") or 0))
    p.add_argument("--template-hash-id", default=os.getenv("TEMPLATE_HASH") or os.getenv("API_SECRET") or template_hash)
    p.add_argument("--series", default=os.getenv("GPU_SERIES","30,40"))
    p.add_argument("--extra-gpu", default=os.getenv("EXTRA_GPU",""))
    p.add_argument("--db-path", default=os.getenv("DB_PATH","./data/vast_prices.duckdb"))
    # ladder
    p.add_argument("--rebid-on-preempt", action="store_true", default=os.getenv("REBID_ON_PREEMPT","1").lower() in {"1","true","yes"})
    p.add_argument("--rebid-steps", default=os.getenv("REBID_STEPS","1.00,1.02,1.05"))
    p.add_argument("--rebid-max-frac-od", type=float, default=float(os.getenv("REBID_MAX_FRAC_OD","0.90")))
    p.add_argument("--rebid-interval-sec", type=int, default=int(os.getenv("REBID_INTERVAL","60")))
    p.add_argument("--warmup-sec", type=int, default=int(os.getenv("WARMUP_SEC","120")))
    p.add_argument("--min-num-gpus", type=int, default=int(os.getenv("MIN_NUM_GPUS", "1")),
                   help="Minimum GPUs per offer to consider (default: 1)")
    p.add_argument("--max-num-gpus", type=int, default=int(os.getenv("MAX_NUM_GPUS", "2")),
                   help="Maximum GPUs per offer to consider (default: 4)")

    args = p.parse_args()

    # build gpu models
    series = {s.strip() for s in args.series.split(",") if s.strip()}
    gpu = []
    if "30" in series: gpu += GPU_30_SERIES
    if "40" in series: gpu += GPU_40_SERIES
    if args.extra_gpu:
        gpu += [g.strip() for g in args.extra_gpu.split(",") if g.strip()]
    gpu = sorted(set(gpu))

    return Config(
        api_key=api_key,
        template_hash_id=(args.template_hash_id or None),
        template_id=(args.template_id or None) if args.template_id else None,
        gpu_models=gpu,
        countries=[c.strip().upper() for c in args.countries.split(",") if c.strip()],
        min_reliability=float(args.min_reliability),
        instance_type=args.instance_type,
        limit=int(args.limit),
        poll_sec=int(args.poll_sec),
        window_min=int(args.window_min),
        discount=float(args.discount),
        max_dph=float(args.max_dph),
        print_top=int(args.print_top),
        auto_rent=bool(args.auto_rent),
        exit_after_rent=bool(args.exit_after_rent),
        max_instances=int(args.max_instances),
        active_states={s.strip().lower() for s in args.active_states.split(",") if s.strip()},
        label_prefix=args.label_prefix,
        rebid_on_preempt=bool(args.rebid_on_preempt),
        rebid_steps=[float(s) for s in args.rebid_steps.split(",") if s.strip()],
        rebid_max_frac_od=float(args.rebid_max_frac_od),
        rebid_interval_sec=int(args.rebid_interval_sec),
        warmup_sec=int(args.warmup_sec),
        db_path=args.db_path,
        min_gpus_per_offer = args.min_num_gpus,
        max_gpus_per_offer = args.max_num_gpus,

    )

# ---------- Class ----------
class VastSniper:
    def __init__(self, cfg: Config):
        self.cfg = cfg
        self.headers = {"Authorization": f"Bearer {cfg.api_key}", "Content-Type": "application/json"}
        os.makedirs(os.path.dirname(cfg.db_path), exist_ok=True)
        self.con = duckdb.connect(cfg.db_path)
        self._init_db()
        self._last_rebid_at = defaultdict(float)
        self._run_stats = {}  # iid -> {"t_start": float|None, "mtti_sec": float|None}

    # --- DB ---
    def _init_db(self):
        self.con.execute("""
        CREATE TABLE IF NOT EXISTS offers (
          ts TIMESTAMP,
          ask_id BIGINT,
          host_id BIGINT,
          machine_id BIGINT,
          gpu_name TEXT,
          num_gpus INTEGER,
          instance_type TEXT,
          dph_total DOUBLE,
          min_bid DOUBLE,
          price_per_gpu DOUBLE,
          reliability DOUBLE,
          geolocation TEXT,
          country TEXT,
          cpu_name TEXT        -- NEW
        );
        """)
        # If the table already existed without cpu_name, add it
        self.con.execute("ALTER TABLE offers ADD COLUMN IF NOT EXISTS cpu_name TEXT;")
        self.con.execute("CREATE INDEX IF NOT EXISTS idx_ts ON offers(ts);")
        self.con.execute("CREATE INDEX IF NOT EXISTS idx_model ON offers(gpu_name);")

    # --- API ---
    def search_offers(self, instance_type: str) -> List[Dict]:
        """
        Query Vast for current offers, filtering on GPU models, reliability,
        and GPU count range [min_gpus_per_offer, max_gpus_per_offer].
        """
        num_filter = {"gte": int(self.cfg.min_gpus_per_offer)}
        if self.cfg.max_gpus_per_offer is not None:
            num_filter["lte"] = int(self.cfg.max_gpus_per_offer)

        payload = {
            "limit": self.cfg.limit,
            "type": instance_type,  # "bid" | "on-demand" | "reserved"
            "verified": {"eq": True},
            "rentable": {"eq": True},
            "rented": {"eq": False},
            "reliability": {"gte": float(self.cfg.min_reliability)},
            "gpu_name": {"in": self.cfg.gpu_models},  # space-form names
            "num_gpus": num_filter,
        }

        r = requests.post(f"{API_BASE}/bundles/", headers=self.headers, json=payload, timeout=30)
        r.raise_for_status()
        data = r.json()
        offers = data.get("offers", [])
        if isinstance(offers, dict):
            offers = [offers] if "id" in offers else list(offers.values())
        return offers

    def list_instances(self) -> List[Dict]:
        r = requests.get(f"{API_BASE}/instances/", headers=self.headers, timeout=30)
        r.raise_for_status()
        return r.json().get("instances", []) or []

    def create_instance(self, ask_id: int, total_price: Optional[float], label: str) -> Optional[Dict]:
        body = {"target_state": "running", "cancel_unavail": True, "label": label}
        if self.cfg.template_hash_id:
            body["template_hash_id"] = self.cfg.template_hash_id
        elif self.cfg.template_id:
            body["template_id"] = int(self.cfg.template_id)
        else:
            print(f"[{ts()}] ERROR: No template provided", file=sys.stderr); return None
        if self.cfg.instance_type == "bid" and total_price is not None:
            body["price"] = float(total_price)
        r = requests.put(f"{API_BASE}/asks/{ask_id}/", headers=self.headers, json=body, timeout=30)
        if not r.ok:
            print(f"[{ts()}] ✖ create failed ({r.status_code}): {r.text}", file=sys.stderr)
            return None
        try: return r.json()
        except Exception: return {"raw": r.text}

    def change_bid(self, instance_id: int, new_total: float) -> bool:
        url = f"{API_BASE}/instances/bid_price/{instance_id}/"
        body = {"price": float(new_total)}
        r = requests.put(url, headers=self.headers, json=body, timeout=15)
        return bool(r.ok)

    # --- Transforms ---
    def offers_to_df(self, offers: List[Dict], instance_type: str) -> pd.DataFrame:
        now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        rows = []
        for o in offers:
            geo = o.get("geolocation") or ""
            ng = int(o.get("num_gpus") or 1)
            dph = o.get("dph_total")
            mb = o.get("min_bid")

            # Try multiple keys; Vast hosts aren’t perfectly uniform
            cpu_str = (
                    o.get("cpu_name")
                    or o.get("cpu_brand")
                    or o.get("cpu_model")
                    or o.get("cpu")
                    or ""
            )

            ppg_ask = (float(dph) / ng) if (dph is not None and ng > 0) else None
            ppg_min = (float(mb) / ng) if (mb is not None and ng > 0) else None
            price_per_gpu = ppg_min if instance_type == "bid" else ppg_ask
            if price_per_gpu is None:
                price_per_gpu = ppg_ask if ppg_ask is not None else ppg_min

            rows.append({
                "ts": now,
                "ask_id": o.get("id") or o.get("ask_contract_id"),
                "host_id": o.get("host_id"),
                "machine_id": o.get("machine_id"),
                "gpu_name": o.get("gpu_name"),
                "num_gpus": ng,
                "instance_type": instance_type,
                "dph_total": float(dph) if dph is not None else None,
                "min_bid": float(mb) if mb is not None else None,
                "price_per_gpu": price_per_gpu,
                "reliability": o.get("reliability"),
                "geolocation": geo,
                "country": country_of(geo),
                "cpu_name": cpu_str,  # NEW
            })

        df = pd.DataFrame(rows)
        if df.empty:
            return df

        # Keep rows with a price and within GPU-count bounds (if you use those)
        df = df[pd.notna(df["price_per_gpu"])]
        if hasattr(self.cfg, "min_gpus_per_offer"):
            df = df[df["num_gpus"] >= int(self.cfg.min_gpus_per_offer)]
        if hasattr(self.cfg, "max_gpus_per_offer") and self.cfg.max_gpus_per_offer is not None:
            df = df[df["num_gpus"] <= int(self.cfg.max_gpus_per_offer)]

        # >>> Exclude: RTX 3090 on Intel/Xeon hosts
        bad_3090_on_intel = (
                (df["gpu_name"] == "RTX 3090") &
                (df["cpu_name"].str.contains(r"intel|xeon", case=False, na=False))
        )
        df = df[~bad_3090_on_intel]

        # (If you also have “only 30-series for multi-GPU” enabled, apply it here too)
        if getattr(self.cfg, "only_30_for_multi", False):
            df = df[(df["num_gpus"] < 2) | (df["gpu_name"].isin(GPU_30_SERIES))]

        return df

    def ingest_offers(self, df: pd.DataFrame):
        if df.empty:
            return
        self.con.register("df_now", df)
        self.con.execute("""
          INSERT INTO offers
          SELECT ts, ask_id, host_id, machine_id, gpu_name, num_gpus, instance_type,
                 dph_total, min_bid, price_per_gpu, reliability, geolocation, country,
                 cpu_name
          FROM df_now
        """)
        self.con.unregister("df_now")

    def compute_baselines(self, instance_type: str) -> Dict[str, Tuple[float, int]]:
        """
        Return {gpu_name: (median_price_per_gpu, sample_count)} over the rolling window,
        restricted to configured countries, reliability, and GPU-count bounds.
        """
        # Build the optional country filter safely (supports empty list)
        if self.cfg.countries:
            country_clause = f"AND country IN ({','.join(['?'] * len(self.cfg.countries))})"
        else:
            country_clause = ""

        # Add GPU-count bounds (always enforce lower bound; upper bound optional)
        upper_bound_clause = "AND num_gpus <= ?" if self.cfg.max_gpus_per_offer is not None else ""

        q = f"""
        WITH recent AS (
          SELECT *
          FROM offers
          WHERE ts >= now() - INTERVAL {self.cfg.window_min} MINUTE
            AND instance_type = ?
            {country_clause}
            AND reliability >= ?
            AND price_per_gpu IS NOT NULL
            AND num_gpus >= ?
            {upper_bound_clause}
        )
        SELECT gpu_name,
               quantile_cont(price_per_gpu, 0.5) AS median_price,
               COUNT(*) AS n
        FROM recent
        GROUP BY 1
        """

        params: List[object] = [instance_type]
        if self.cfg.countries:
            params += list(self.cfg.countries)
        params += [float(self.cfg.min_reliability), int(self.cfg.min_gpus_per_offer)]
        if self.cfg.max_gpus_per_offer is not None:
            params.append(int(self.cfg.max_gpus_per_offer))

        df = self.con.execute(q, params).df()
        return {r["gpu_name"]: (float(r["median_price"]), int(r["n"])) for _, r in df.iterrows()}

    # --- Selection & states ---
    @staticmethod
    def _norm_state(ins: Dict) -> str:
        for k in ("cur_state","actual_status","next_state"):
            v = (ins.get(k) or "").lower()
            if v: return v
        return ""

    @staticmethod
    def _instance_total_rate(ins: Dict) -> Optional[float]:
        for k in ("price","dph_total","total_dph","instance_price"):
            v = ins.get(k)
            if isinstance(v,(int,float)): return float(v)
        return None

    def _note_running(self, ins: Dict):
        iid = int(ins.get("id"))
        now = time.time()
        st = self._norm_state(ins)
        rs = self._run_stats.get(iid) or {"t_start": None, "mtti_sec": None}
        if st == "running" and rs["t_start"] is None:
            rs["t_start"] = now
        self._run_stats[iid] = rs

    def _note_preempted(self, ins: Dict, alpha=0.3):
        iid = int(ins.get("id"))
        now = time.time()
        rs = self._run_stats.get(iid)
        if not rs: return
        t0 = rs.get("t_start")
        if t0:
            dur = now - t0
            prev = rs.get("mtti_sec")
            rs["mtti_sec"] = dur if prev is None else (alpha*dur + (1-alpha)*prev)
            rs["t_start"] = None
            self._run_stats[iid] = rs

    # --- Deal selection ---
    def select_deals(self, df_now: pd.DataFrame, baselines: Dict[str, Tuple[float,int]]) -> List[Dict]:
        if df_now.empty: return []
        cur = df_now[
            (df_now["country"].isin(self.cfg.countries)) &
            (pd.to_numeric(df_now["reliability"], errors="coerce").fillna(0) >= self.cfg.min_reliability)
        ].copy()
        if self.cfg.max_dph and self.cfg.max_dph>0:
            # total price cap using whichever is present
            price_now = cur["dph_total"].where(pd.notna(cur["dph_total"]), cur["min_bid"])
            cur = cur[price_now <= self.cfg.max_dph]
        deals = []
        for _, r in cur.iterrows():
            model = r["gpu_name"]
            base = baselines.get(model)
            if not base: continue
            median_price, n = base
            price = float(r["price_per_gpu"])
            if median_price and price <= (1.0 - self.cfg.discount) * median_price:
                deals.append({
                    "ask_id": int(r["ask_id"]),
                    "gpu_name": model,
                    "num_gpus": int(r["num_gpus"]),
                    "price_per_gpu": price,
                    "median_price": median_price,
                    "pct_below": 1.0 - (price/median_price),
                    "country": r["country"],
                    "reliability": float(r["reliability"]) if pd.notna(r["reliability"]) else None,
                    "dph_total": float(r["dph_total"]) if pd.notna(r["dph_total"]) else None,
                    "min_bid": float(r["min_bid"]) if pd.notna(r["min_bid"]) else None,
                })
        deals.sort(key=lambda d: (-d["pct_below"], d["price_per_gpu"]))
        return deals

    def _apply_multi_gpu_policy(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty or not getattr(self.cfg, "only_30_when_multi", False):
            return df
        # GPU_30_SERIES is your existing constant list with space-form names
        mask_drop = (df["num_gpus"] >= 2) & (~df["gpu_name"].isin(GPU_30_SERIES))
        return df[~mask_drop]

    # --- Rebid ladder ---
    def rebid_paused_instances(self, od_baselines: Dict[str, Tuple[float,int]]):
        now = time.time()
        ins_list = self.list_instances()
        for ins in ins_list:
            st = self._norm_state(ins)
            lbl = ins.get("label","")
            if st == "running":
                self._note_running(ins)
                continue
            if not any(k in st for k in ("paused","preempt","outbid","stopped")):
                continue

            # mark interruption
            self._note_preempted(ins)

            iid = int(ins.get("id")); ng = int(ins.get("num_gpus") or 1)
            model = ins.get("gpu_name") or ins.get("gpu_type") or ""
            if now - self._last_rebid_at[iid] < self.cfg.rebid_interval_sec:
                continue

            cur_total = self._instance_total_rate(ins) or 0.0
            od_med = od_baselines.get(model, (None,0))[0]
            cap_total = od_med*ng*self.cfg.rebid_max_frac_od if od_med is not None else None

            # optional break-even cap from observed MTTI
            rs = self._run_stats.get(iid) or {}
            mtti = rs.get("mtti_sec")
            if (mtti and od_med is not None):
                be_total = (od_med*ng) * max(0.0, 1.0 - self.cfg.warmup_sec/max(mtti,1.0))
                cap_total = be_total if cap_total is None else min(cap_total, be_total)

            print(f"[{ts()}] {lbl} preempted; current=${cur_total:.6f}/h; cap={cap_total if cap_total else 'n/a'}")
            for mult in self.cfg.rebid_steps:
                if cur_total <= 0: break
                new_total = round_up(cur_total * mult, 6)
                if cap_total is not None and new_total > cap_total:
                    print(f"[{ts()}] ladder would exceed cap ({new_total} > {cap_total}); stop.")
                    break
                ok = self.change_bid(iid, new_total)
                print(f"[{ts()}] change-bid id={iid} → ${new_total:.6f}/h {'OK' if ok else 'FAIL'}")
                time.sleep(3)
                # re-read
                ins2 = next((x for x in self.list_instances() if int(x.get("id"))==iid), None)
                if ins2 and self._norm_state(ins2) == "running":
                    print(f"[{ts()}] {lbl} resumed at ${new_total:.6f}/h")
                    break

            self._last_rebid_at[iid] = now

    def _available_quota(self) -> int:
        """
        How many new instances we are allowed to create this loop given the cap.
        Returns at least 1 if no cap is configured.
        """
        if not self.cfg.max_instances:
            return 1
        active = 0
        for ins in self.list_instances():
            st = (ins.get("cur_state") or ins.get("actual_status") or "").lower()
            if st in self.cfg.active_states:
                active += 1
        return max(0, self.cfg.max_instances - active)

    def _rent_from_deals(self, deals: List[Dict], quota: int) -> int:
        """
        Try to rent up to `quota` instances by walking the ranked deals list.
        Returns number of successful rents this call.
        """
        if quota <= 0 or not deals:
            return 0

        rented = 0
        tried = 0
        for pick in deals:
            if rented >= quota:
                break

            label = f"{self.cfg.label_prefix}-{pick['gpu_name'].replace(' ', '_')}-{pick['price_per_gpu']:.3f}"

            # Compute BID total (if in bid mode), enforcing the current floor
            ng = int(pick["num_gpus"])
            total = float(pick["price_per_gpu"]) * ng
            min_bid_total = float(pick["min_bid"]) if pick.get("min_bid") is not None else 0.0

            # Round up to avoid equality misses at the floor
            bid_total = max(self._round_up(total), min_bid_total) if self.cfg.instance_type == "bid" else None

            print(f"[{ts()}] rent try {tried + 1}: ask {pick['ask_id']} | {pick['gpu_name']} x{ng} | "
                  f"ppg=${pick['price_per_gpu']:.6f} | "
                  f"{'bid_total=$' + format(bid_total, '.6f') if bid_total is not None else 'on-demand'}")

            resp = self.create_instance(
                ask_id=int(pick["ask_id"]),
                total_price=bid_total,
                label=label
            )

            tried += 1

            if resp:
                rented += 1
                print(f"[{ts()}] ✔ rented ask {pick['ask_id']} → {resp}")
                if self.cfg.exit_after_rent:
                    sys.exit(0)
            else:
                # create_instance() already printed the HTTP error (e.g., 410 no_such_ask)
                print(f"[{ts()}] ✖ rent failed for ask {pick['ask_id']} — trying next deal...")

        if rented == 0:
            print(f"[{ts()}] No deals could be rented this cycle (tried {tried}).")
        return rented

    # local round-up (reuses your earlier helper if you already defined round_up)
    def _round_up(self, x: float, places: int = 6) -> float:
        scale = 10 ** places
        return (int(x * scale + 0.999999) / scale)

    # --- One loop ---
    def loop_once(self):
        # enforce cap (skip the whole cycle if we're already full)
        if self.cfg.max_instances:
            cur_list = self.list_instances()
            active = sum(1 for ins in cur_list
                         if (ins.get("cur_state") or ins.get("actual_status") or "").lower()
                         in self.cfg.active_states)
            if active >= self.cfg.max_instances:
                print(f"[{ts()}] Cap reached {active}/{self.cfg.max_instances}.")
                return

        # pull BID and OD snapshots
        bid_offers = self.search_offers("bid")
        od_offers = self.search_offers("on-demand")

        # build frames and ingest
        df_bid = self.offers_to_df(bid_offers, "bid")
        df_od = self.offers_to_df(od_offers, "on-demand")

        if not df_bid.empty:
            self.ingest_offers(df_bid)

        if not df_od.empty:
            self.ingest_offers(df_od)

        # compute baselines
        baselines_bid = self.compute_baselines("bid")
        baselines_od = self.compute_baselines("on-demand")

        # rebid paused/outbid instances before renting new capacity
        if self.cfg.rebid_on_preempt:
            self.rebid_paused_instances(baselines_od)

        # show baselines
        if baselines_bid:
            print(f"\n[{ts()}] baselines (rolling {self.cfg.window_min}m, BID price_per_gpu):")
            for m in sorted(baselines_bid.keys()):
                med, n = baselines_bid[m]
                print(f"  {m:18s} median=${med:.3f} (n={n})")
        else:
            print(f"\n[{ts()}] baselines: (no data yet)")

        # select deals from current BID offers
        deals = self.select_deals(df_bid, baselines_bid)
        if deals:
            print(f"\n[{ts()}] DEALS (≥{self.cfg.discount * 100:.0f}% below baseline):")
            for d in deals[:self.cfg.print_top]:
                rel_s = f"{d['reliability']:.3f}" if isinstance(d['reliability'], (int, float)) else "n/a"
                print(f"  ask {d['ask_id']} | {d['gpu_name']:18s} | ppg=${d['price_per_gpu']:.3f}/h "
                      f"| base=${d['median_price']:.3f} ({d['pct_below'] * 100:.1f}% off) "
                      f"| {d['country']} | rel={rel_s}")
        else:
            print(f"\n[{ts()}] No deals at ≥{self.cfg.discount * 100:.0f}% off right now.")

        # rent from the ranked list (iterate through candidates this loop)
        if deals and self.cfg.auto_rent:
            quota = self._available_quota()
            if quota <= 0:
                print(f"[{ts()}] Cap reached during rent attempt.")
                return

            # choose how aggressively to fill — 1 per loop, or all remaining capacity
            quota_to_use = 1  # conservative; avoids too many PUTs in a single cycle
            # quota_to_use = quota               # uncomment to fill all capacity at once

            rented = self._rent_from_deals(deals, quota_to_use)
            if rented and self.cfg.exit_after_rent:
                sys.exit(0)

    def run(self):
        print(f"[{ts()}] GPUs: {', '.join(self.cfg.gpu_models)}")
        print(f"[{ts()}] Region: {self.cfg.countries} | type={self.cfg.instance_type} | "
              f"min_rel={self.cfg.min_reliability} | window={self.cfg.window_min}m | discount={self.cfg.discount}")
        if self.cfg.max_dph: print(f"[{ts()}] Hard cap: max_dph=${self.cfg.max_dph:.3f}/h")
        if self.cfg.max_instances: print(f"[{ts()}] Cap: max_instances={self.cfg.max_instances} ({sorted(self.cfg.active_states)})")
        print(f"[{ts()}] Using template: "
              f"{'hash='+self.cfg.template_hash_id if self.cfg.template_hash_id else ('id='+str(self.cfg.template_id) if self.cfg.template_id else 'ERROR (none)')}")

        while True:
            try:
                self.loop_once()
            except requests.HTTPError as e:
                body = getattr(e.response, "text", None)
                print(f"[{ts()}] HTTP error: {e} / {body}", file=sys.stderr)
            except Exception as e:
                print(f"[{ts()}] Error: {e}", file=sys.stderr)
            time.sleep(self.cfg.poll_sec)

# ---- Entrypoint ----
if __name__ == "__main__":
    cfg = parse_args_to_config()
    VastSniper(cfg).run()