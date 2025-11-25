# offer_store.py
from __future__ import annotations
import os
from dataclasses import dataclass, field
from typing import Dict, Tuple, List, Optional
from datetime import datetime
import duckdb
import pandas as pd

@dataclass
class OfferStore:
    db_path: str
    _con: duckdb.DuckDBPyConnection = field(init=False, repr=False)

    def __post_init__(self):
        dirpath = os.path.dirname(self.db_path)
        if dirpath:
            os.makedirs(dirpath, exist_ok=True)
        self._con = duckdb.connect(self.db_path)
        self._init_schema()

    def close(self):
        try:
            self._con.close()
        except Exception:
            pass

    def __del__(self):
        self.close()

    def _init_schema(self):
        self._con.execute("""
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
          cpu_name TEXT
        );
        """)
        self._con.execute("CREATE INDEX IF NOT EXISTS idx_ts ON offers(ts);")
        self._con.execute("CREATE INDEX IF NOT EXISTS idx_model ON offers(gpu_name);")

    # Ingest current market snapshot into rolling table
    def ingest(self, df: pd.DataFrame) -> int:
        if df is None or df.empty:
            return 0
        self._con.register("df_now", df)
        self._con.execute("""
            INSERT INTO offers
            SELECT ts, ask_id, host_id, machine_id, gpu_name, num_gpus, instance_type,
                   dph_total, min_bid, price_per_gpu, reliability, geolocation, country, cpu_name
            FROM df_now
        """)
        self._con.unregister("df_now")
        return len(df)

    # Sliding-window medians by model with filters applied server-side
    def compute_baselines(
        self,
        *,
        instance_type: str,
        window_min: int,
        countries: List[str],
        min_reliability: float,
        min_gpus_per_offer: int,
        max_gpus_per_offer: Optional[int],
    ) -> Dict[str, Tuple[float, int]]:
        country_clause = f"AND country IN ({','.join(['?'] * len(countries))})" if countries else ""
        upper_bound_clause = "AND num_gpus <= ?" if max_gpus_per_offer is not None else ""
        q = f"""
        WITH recent AS (
          SELECT *
          FROM offers
          WHERE ts >= now() - INTERVAL {int(window_min)} MINUTE
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
        if countries:
            params += list(countries)
        params += [float(min_reliability), int(min_gpus_per_offer)]
        if max_gpus_per_offer is not None:
            params.append(int(max_gpus_per_offer))
        df = self._con.execute(q, params).df()
        return {r["gpu_name"]: (float(r["median_price"]), int(r["n"])) for _, r in df.iterrows()}

    def prune_older_than(self, days: int = 7):
        self._con.execute(f"DELETE FROM offers WHERE ts < now() - INTERVAL {int(days)} DAY")
