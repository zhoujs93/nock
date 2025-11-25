from dataclasses import dataclass, field
from typing import Dict, List, Optional, Set
# ------------------------------- Config (YAML) --------------------------------
GPU_30_SERIES = [
    "RTX 3080", "RTX 3080 Ti",
    "RTX 3090", "RTX 3090 Ti",
]
GPU_40_SERIES = [
    "RTX 4070", "RTX 4070 Ti", "RTX 4070 Super", "RTX 4070 Ti Super",
    "RTX 4080", "RTX 4080 Super", "RTX 4090", "RTX 4090D"
]
GPU_50_SERIES = [
    "RTX 5070", "RTX 5070 Ti", "RTX 5070 Super", "RTX 5070 Ti Super",
    "RTX 5080", "RTX 5080 Super", "RTX 5090", "RTX 5090D"
]

@dataclass
class Config:
    # Required
    api_key: str
    template_hash_id: Optional[str] = None
    template_id: Optional[int] = None

    # Offers & markets
    series: List[str] = None
    gpu_models: List[str] = field(default_factory=lambda: sorted(set(GPU_30_SERIES + GPU_40_SERIES)))
    countries: List[str] = field(default_factory=lambda: ["US", "CA"])
    regions: List[str] = field(default_factory=list)
    sparse_models: List[str] = field(default_factory=list)
    min_reliability: float = 0.95
    min_cuda: float = 12.6
    instance_type: str = "bid"  # "bid" | "on-demand" | "reserved"
    limit: int = 200
    poll_sec: int = 12
    window_min: int = 120
    max_dph: float = 0.0       # 0=off (also used by cleaner if max_dph_keep=0)
    print_top: int = 8
    gpu_filter_mode: str = "server"  # "server" (current) | "client"
    min_samples_per_baseline: int = 2  # was hard-coded 5

    # bidding parameters:
    # how many samples are "enough" to trust a median
    min_samples_per_baseline_bid: int = 8
    min_samples_per_baseline_od: int = 8
    od_anchor_frac: float = 0.85  # cap vs OD: accept if ≤85% of OD median
    discount: float = 0.25
    sparse_od_min_frac: float = 1.0
    sparse_min_total_samples: int = 20  # use OD-min policy if (n_bid + n_od) < this

    # Renting & capacity
    auto_rent: bool = True
    exit_after_rent: bool = False
    max_instances: int = 150
    active_states: Set[str] = field(default_factory=lambda: {"running", "starting", "restarting"})
    label_prefix: str = "deal-snipe"

    # Ladder / rebids
    rebid_on_preempt: bool = True
    rebid_steps: List[float] = field(default_factory=lambda: [1.00, 1.02, 1.05])
    rebid_max_frac_od: float = 0.85
    rebid_interval_sec: int = 60
    warmup_sec: int = 120
    bid_jitter: float = 0.000005

    # DB
    db_path: str = "./data/vast_prices.duckdb"

    # GPU count filters
    min_gpus_per_offer: int = 1
    max_gpus_per_offer: Optional[int] = 4
    only_30_for_multi: bool = False

    # Cleaner (price-only)
    min_gpu_util_keep: float = 1.0  # (placeholder for util checks if you wire them)
    low_util_hold_sec: int = 0
    min_discount_keep: float = 0.10
    clean_action: str = "delete"    # "delete" | "stop"
    clean_every_loops: int = 1
    max_dph_keep: float = 0.0       # if 0, uses max_dph when >0

    # Detail limits (for optional GET detail/telemetry)
    clean_batch_per_loop: int = 16
    detail_batch_per_loop: int = 6
    detail_min_delay_sec: float = 0.6

    # Telemetry (optional)
    telemetry_url: str = "http://127.0.0.1:9100/"
    telemetry_timeout: float = 3.0
    cpu_fallback_hold_sec: int = 90
    log_level: str = "INFO"

    # Stuck-state timers + actions
    outbid_hold_sec: int = 1800      # 30m
    scheduling_hold_sec: int = 600   # 10m
    on_outbid_action: str = "kill"   # "keep" | "kill" | "rebid"
    on_sched_action: str = "kill"    # "keep" | "kill"

    # (Optional) per-endpoint rate-limits (RPS)
    rate_limits: Dict[str, float] = field(default_factory=dict)

    rents_per_loop: int = 1
    burst_top_k: int = 2
    overshoot_kill: bool = True
    attempt_all_deals: bool = True
    burst_waves: int = 1
    ask_410_cooldown_sec: float = 45