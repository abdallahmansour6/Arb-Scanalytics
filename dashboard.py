"""
Live radar dashboard. Streamlit + Plotly + DuckDB over the collector's
Parquet partitions.

Run:
    streamlit run dashboard.py

VS Code Remote-SSH auto-detects the listening port (default 8501) and
surfaces a 'Open in browser' toast that opens it through the SSH tunnel.

Rate semantics (display labels mirror SCHEMA in collector.py):
  - "Rate (next epoch)"     = upcoming-boundary rate, what trades pay/receive
                              at next_funding_ts. (CCXT unified.fundingRate)
  - "Forecast (cycle after)" = exchange's prediction for the cycle AFTER the
                               upcoming one. Null on most venues; populated on
                               BITMART, COINEX, OKX, PHEMEX (and HTX when the
                               venue fills info.estimated_rate).
"""

import logging
import time
from datetime import datetime, timezone

import duckdb
import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from config import FUNDING_DIR

# Field-notes: VS Code Remote-SSH port-forward probes generate
# 'Invalid HTTP request received' warnings from tornado. Silence; benign.
logging.getLogger("tornado.general").setLevel(logging.ERROR)


_GLOB = str(FUNDING_DIR / "**" / "*.parquet").replace("\\", "/")


@st.cache_resource
def _db():
    db = duckdb.connect()
    db.sql(f"""create or replace view f as
               select * from read_parquet('{_GLOB}',
                                          hive_partitioning=true,
                                          union_by_name=true)""")
    return db


@st.cache_data(ttl=30)
def _latest_snapshot() -> pd.DataFrame:
    """One row per (exchange, symbol_canonical) — the most recent observation,
    with a global oi_rank assigned descending by open_interest_usd. Rows with
    null OI are pushed to the bottom of the rank order."""
    return _db().sql("""
        with latest as (
            select * from f
            qualify row_number() over (partition by exchange, symbol_canonical
                                       order by ts_utc desc) = 1
        )
        select *,
               row_number() over (
                   order by coalesce(open_interest_usd, 0) desc, exchange
               ) as oi_rank
        from latest
    """).df()


@st.cache_data(ttl=30)
def _history(symbol: str, hours: int) -> pd.DataFrame:
    return _db().execute("""
        select ts_utc, exchange, funding_rate,
               apy_norm * 100 as apy_pct,
               mark_price, volume_24h_usd
        from f
        where symbol_canonical = ?
          and ts_utc >= (select max(ts_utc) from f) - (? * 3600 * 1000)
        order by ts_utc
    """, [symbol, hours]).fetchdf()


def _add_countdown(df: pd.DataFrame, now_ms: int) -> pd.DataFrame:
    df = df.copy()
    df["settles_in_min"] = (df["next_funding_ts"] - now_ms) / 60_000.0
    df.loc[df["settles_in_min"] < 0, "settles_in_min"] = pd.NA
    return df


def _column_config(extra: dict | None = None) -> dict:
    cfg = {
        "exchange":         st.column_config.TextColumn("Exchange"),
        "symbol_canonical": st.column_config.TextColumn("Symbol"),
        "funding_rate":     st.column_config.NumberColumn("Rate (next epoch)", format="%.6f"),
        "predicted_rate":   st.column_config.NumberColumn("Forecast (cycle after)", format="%.6f"),
        "funding_interval_h": st.column_config.NumberColumn("Cycle h", format="%.0f"),
        "apy_pct":          st.column_config.NumberColumn("APY %", format="%.1f"),
        "settles_in_min":   st.column_config.NumberColumn("Settles in (m)", format="%.0f"),
        "vol_musd":         st.column_config.NumberColumn("Vol $M", format="%.1f"),
        "oi_musd":          st.column_config.NumberColumn("OI $M", format="%.1f"),
        "oi_rank":          st.column_config.NumberColumn("OI rank", format="%d"),
        "n_venues":         st.column_config.NumberColumn("Venues", format="%d"),
        "venue_high":       st.column_config.TextColumn("Venue (APY high)"),
        "venue_low":        st.column_config.TextColumn("Venue (APY low)"),
        "apy_high_pct":     st.column_config.NumberColumn("APY high %", format="%.1f"),
        "apy_low_pct":      st.column_config.NumberColumn("APY low %", format="%.1f"),
        "delta_apy_pct":    st.column_config.NumberColumn("ΔAPY %", format="%.1f"),
        "min_vol_musd":     st.column_config.NumberColumn("Min vol $M", format="%.1f"),
        "max_vol_musd":     st.column_config.NumberColumn("Max vol $M", format="%.1f"),
        "worst_leg_oi_rank": st.column_config.NumberColumn("Worst leg OI rank", format="%d"),
        "best_leg_oi_rank":  st.column_config.NumberColumn("Best leg OI rank", format="%d"),
    }
    if extra:
        cfg.update(extra)
    return cfg


def main():
    st.set_page_config(page_title="Arb Scanalytics", layout="wide",
                       initial_sidebar_state="expanded")
    st.title("Arb Scanalytics — Live Radar")

    snapshot = _latest_snapshot()
    if snapshot.empty:
        st.warning("No data yet. Run `python collector.py` (or `--once` for one cycle).")
        return

    last_ts = pd.to_datetime(snapshot["ts_utc"].max(), unit="ms", utc=True)
    now_ms = int(time.time() * 1000)
    age_s = (datetime.now(timezone.utc) - last_ts).total_seconds()
    snapshot = _add_countdown(snapshot, now_ms)
    total_pairs = len(snapshot)

    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Venues", snapshot["exchange"].nunique())
    c2.metric("Symbols", snapshot["symbol_canonical"].nunique())
    c3.metric("Pairs", f"{total_pairs:,}")
    c4.metric("Latest cycle", f"{age_s:.0f}s ago")
    if c5.button("Refresh", width="stretch"):
        st.cache_data.clear()
        st.rerun()

    # --- sidebar filters ---------------------------------------------------
    with st.sidebar:
        st.subheader("Filters")
        st.caption("Rank: 1 = highest OI globally. Use the high end as a depth gate.")
        oi_rank_range = st.slider(
            "OI rank range", 1, max(total_pairs, 1),
            (1, min(500, total_pairs)),
        )
        vol_range = st.slider(
            "24h volume range ($M)",
            0.0, 5000.0, (1.0, 5000.0), step=1.0,
        )
        max_settles_min = st.slider(
            "Settles within (min)  — 0 = no limit",
            0, 720, 0, step=15,
        )
        min_venues = st.slider("Min venues for spread", 2, 13, 2)
        st.caption(f"Last update: {last_ts.strftime('%H:%M:%S UTC')}")

    # apply filters to base snapshot
    vol_lo_usd, vol_hi_usd = vol_range[0] * 1e6, vol_range[1] * 1e6
    base = snapshot[
        snapshot["oi_rank"].between(oi_rank_range[0], oi_rank_range[1])
        & snapshot["volume_24h_usd"].fillna(0).between(vol_lo_usd, vol_hi_usd)
    ]
    if max_settles_min > 0:
        base = base[base["settles_in_min"].fillna(1e18) <= max_settles_min]

    tab_anom, tab_spread, tab_hist = st.tabs(
        ["Anomalies", "Cross-venue spreads", "Symbol history"]
    )

    # --- Anomalies ---------------------------------------------------------
    with tab_anom:
        st.subheader("Top anomalies by |APY|")
        df = base.copy()
        df["apy_pct"] = df["apy_norm"] * 100
        df["vol_musd"] = df["volume_24h_usd"] / 1e6
        df["oi_musd"] = df["open_interest_usd"] / 1e6
        df = df.assign(_abs=df["apy_pct"].abs()).sort_values("_abs", ascending=False).drop("_abs", axis=1)
        cols = ["exchange", "symbol_canonical", "funding_rate", "funding_interval_h",
                "predicted_rate", "apy_pct", "settles_in_min",
                "vol_musd", "oi_musd", "oi_rank"]
        st.caption(f"{len(df):,} pairs match filters")
        st.dataframe(
            df[cols], width="stretch", hide_index=True, height=600,
            column_config=_column_config(),
        )

    # --- Cross-venue spreads ----------------------------------------------
    with tab_spread:
        st.subheader("Symbols ranked by cross-venue ΔAPY")
        # Aggregate filtered base into per-symbol rows.
        agg = (
            base.groupby("symbol_canonical")
            .agg(
                n_venues=("exchange", "count"),
                apy_high=("apy_norm", "max"),
                apy_low=("apy_norm", "min"),
                min_vol_usd=("volume_24h_usd", "min"),
                max_vol_usd=("volume_24h_usd", "max"),
                best_leg_oi_rank=("oi_rank", "min"),
                worst_leg_oi_rank=("oi_rank", "max"),
                min_settles=("settles_in_min", "min"),
            )
            .reset_index()
        )
        # which venues sit at the high/low APY for each symbol
        idx_high = base.groupby("symbol_canonical")["apy_norm"].idxmax()
        idx_low = base.groupby("symbol_canonical")["apy_norm"].idxmin()
        agg["venue_high"] = base.loc[idx_high, "exchange"].values
        agg["venue_low"] = base.loc[idx_low, "exchange"].values

        agg = agg[agg["n_venues"] >= min_venues].copy()
        agg["apy_high_pct"] = agg["apy_high"] * 100
        agg["apy_low_pct"] = agg["apy_low"] * 100
        agg["delta_apy_pct"] = agg["apy_high_pct"] - agg["apy_low_pct"]
        agg["min_vol_musd"] = agg["min_vol_usd"] / 1e6
        agg["max_vol_musd"] = agg["max_vol_usd"] / 1e6
        agg["settles_in_min"] = agg["min_settles"]
        agg = agg.sort_values("delta_apy_pct", ascending=False)
        cols = ["symbol_canonical", "n_venues", "venue_high", "venue_low",
                "apy_high_pct", "apy_low_pct", "delta_apy_pct",
                "settles_in_min", "min_vol_musd", "max_vol_musd",
                "worst_leg_oi_rank", "best_leg_oi_rank"]
        st.caption(f"{len(agg):,} symbols on ≥{min_venues} venues match filters")
        st.dataframe(
            agg[cols], width="stretch", hide_index=True, height=600,
            column_config=_column_config(),
        )

    # --- Symbol history ----------------------------------------------------
    with tab_hist:
        st.subheader("Funding rate history overlaid across venues")
        symbols = sorted(snapshot["symbol_canonical"].unique())
        default_sym = "BTC/USDT:USDT" if "BTC/USDT:USDT" in symbols else symbols[0]
        col_a, col_b = st.columns([2, 1])
        with col_a:
            sym = st.selectbox("Symbol", symbols, index=symbols.index(default_sym))
        with col_b:
            hours = st.slider("Hours of history", 1, 72, 6)

        venues_avail = sorted(
            snapshot.loc[snapshot["symbol_canonical"] == sym, "exchange"].unique()
        )
        chosen = st.multiselect("Venues", venues_avail, default=venues_avail)

        hist = _history(sym, hours)
        if hist.empty:
            st.info("No history for this symbol yet.")
            return
        hist = hist[hist["exchange"].isin(chosen)]
        hist["ts_utc"] = pd.to_datetime(hist["ts_utc"], unit="ms", utc=True)

        fig = go.Figure()
        for venue in chosen:
            d = hist[hist["exchange"] == venue]
            fig.add_trace(go.Scatter(
                x=d["ts_utc"], y=d["apy_pct"],
                mode="lines+markers", name=venue,
                hovertemplate=(
                    "%{x|%H:%M:%S} — %{y:.1f}%%<extra>" + venue + "</extra>"
                ),
            ))
        fig.update_layout(
            xaxis_title="Time (UTC)", yaxis_title="APY (%)",
            hovermode="x unified", height=500,
            margin=dict(l=20, r=20, t=20, b=20),
        )
        st.plotly_chart(fig, width="stretch")
        st.caption(f"{len(hist):,} observations across {len(chosen)} venues")


if __name__ == "__main__":
    main()
