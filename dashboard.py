"""
Live radar dashboard. Streamlit + Plotly + DuckDB over the collector's
Parquet partitions.

Run:
    streamlit run dashboard.py

VS Code Remote-SSH auto-detects the listening port (default 8501) and
surfaces a 'Open in browser' toast that opens it through the SSH tunnel.

Refresh mechanics:
  - Header (top metrics + freshness dot) auto-reruns every 15 s.
  - Each tab's data tables auto-rerun every 30 s via st.fragment.
  - Filter widgets are OUTSIDE the fragments — typing in a number_input
    doesn't get clobbered by an auto-rerun.
  - Underlying queries are @st.cache_data(ttl=30); the fragment rerun
    races the cache TTL so a fresh query happens roughly every 30 s.

Rate semantics (display labels mirror SCHEMA in collector.py):
  - "Rate (next epoch)"      = upcoming-boundary rate (B), what trades
                               pay/receive at next_funding_ts.
  - "Forecast (cycle after)" = exchange's prediction for the cycle AFTER
                               upcoming (C). Null on most venues.

Funding-arb directionality:
  - Short the venue with HIGHER APY (positive funding => shorts receive)
  - Long the venue with LOWER APY (negative funding => longs receive)
"""

import logging
import time
from datetime import datetime, timezone

import duckdb
import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from plotly.subplots import make_subplots

from config import FUNDING_DIR

# Field-notes: VS Code Remote-SSH port-forward probes generate
# 'Invalid HTTP request received' warnings from tornado. Silence; benign.
logging.getLogger("tornado.general").setLevel(logging.ERROR)


_GLOB = str(FUNDING_DIR / "**" / "*.parquet").replace("\\", "/")


# --------------------------------------------------------------------------
# data access
# --------------------------------------------------------------------------

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
    """One row per (exchange, symbol_canonical) — most recent observation,
    with global oi_rank descending by open_interest_usd. Null OI sorts to
    the bottom of the rank order."""
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
        select ts_utc, exchange, funding_rate, funding_interval_h,
               apy_norm * 100 as apy_pct, mark_price, volume_24h_usd
        from f
        where symbol_canonical = ?
          and ts_utc >= (select max(ts_utc) from f) - (? * 3600 * 1000)
        order by ts_utc
    """, [symbol, hours]).fetchdf()


# --------------------------------------------------------------------------
# helpers
# --------------------------------------------------------------------------

def _now_ms() -> int:
    return int(time.time() * 1000)


def _add_countdown(df: pd.DataFrame, now_ms: int) -> pd.DataFrame:
    df = df.copy()
    df["settles_in_min"] = (df["next_funding_ts"] - now_ms) / 60_000.0
    df.loc[df["settles_in_min"] < 0, "settles_in_min"] = pd.NA
    return df


def _freshness_dot(age_s: float) -> str:
    """🟢 fresh (within ~2 cycles), 🟡 missed one, 🔴 stale."""
    if age_s < 120:
        return "🟢"
    if age_s < 240:
        return "🟡"
    return "🔴"


def _column_config(extra: dict | None = None) -> dict:
    cfg = {
        "exchange":           st.column_config.TextColumn("Exchange"),
        "symbol_canonical":   st.column_config.TextColumn("Symbol"),
        "funding_rate":       st.column_config.NumberColumn("Rate (next epoch)", format="%.6f"),
        "predicted_rate":     st.column_config.NumberColumn("Forecast (cycle after)", format="%.6f"),
        "funding_interval_h": st.column_config.NumberColumn("Cycle h", format="%.0f"),
        "cycles_h":           st.column_config.TextColumn(
                                  "Cycles h",
                                  help="Funding-cycle hours at the short / long legs. Single value if both legs match."),
        "apy_pct":            st.column_config.NumberColumn("APY %", format="%.1f"),
        "settles_in_min":     st.column_config.NumberColumn("Settles in (m)", format="%.0f"),
        "vol_musd":           st.column_config.NumberColumn("Vol $M", format="%.1f"),
        "oi_musd":            st.column_config.NumberColumn("OI $M", format="%.1f"),
        "oi_rank":            st.column_config.NumberColumn("OI rank", format="%d",
                                                            help="1 = highest OI globally"),
        "n_venues":           st.column_config.NumberColumn("Venues", format="%d"),
        "venue_short":        st.column_config.TextColumn(
                                  "Short venue (APY high)",
                                  help="Short this venue: positive funding => shorts receive"),
        "venue_long":         st.column_config.TextColumn(
                                  "Long venue (APY low)",
                                  help="Long this venue: negative funding => longs receive"),
        "apy_high_pct":       st.column_config.NumberColumn("APY high %", format="%.1f"),
        "apy_low_pct":        st.column_config.NumberColumn("APY low %", format="%.1f"),
        "delta_apy_pct":      st.column_config.NumberColumn("ΔAPY %", format="%.1f"),
        "min_vol_musd":       st.column_config.NumberColumn("Min vol $M", format="%.1f"),
        "max_vol_musd":       st.column_config.NumberColumn("Max vol $M", format="%.1f"),
        "worst_leg_oi_rank":  st.column_config.NumberColumn("Worst leg OI rank", format="%d"),
        "best_leg_oi_rank":   st.column_config.NumberColumn("Best leg OI rank", format="%d"),
    }
    if extra:
        cfg.update(extra)
    return cfg


def _filter_inputs(prefix: str, snapshot: pd.DataFrame) -> dict:
    """Render the four shared filter widgets (OI rank min/max, vol min/max,
    settles_within) for one tab. Returns the resolved values. Clamped to
    the data's actual ranges, with hint captions."""
    total = len(snapshot)
    max_vol_musd = float(snapshot["volume_24h_usd"].max() or 0) / 1e6

    cols = st.columns([1, 1, 1, 1, 1])
    with cols[0]:
        oi_min = st.number_input(
            "OI rank ≥", min_value=1, max_value=total, step=10, value=1,
            key=f"{prefix}_oi_min",
        )
    with cols[1]:
        default_oi_max = min(500, total)
        oi_max = st.number_input(
            "OI rank ≤", min_value=1, max_value=total, step=10,
            value=default_oi_max, key=f"{prefix}_oi_max",
        )
    with cols[2]:
        vol_min_musd = st.number_input(
            "Vol ≥ ($M)", min_value=0.0, value=1.0, step=1.0,
            key=f"{prefix}_vol_min",
        )
    with cols[3]:
        vol_max_musd = st.number_input(
            "Vol ≤ ($M)", min_value=0.0,
            value=float(round(max_vol_musd + 1, 0)), step=10.0,
            key=f"{prefix}_vol_max",
        )
    with cols[4]:
        settles_max = st.number_input(
            "Settles within (m, 0=off)", min_value=0, max_value=720, value=0, step=15,
            key=f"{prefix}_settles_max",
        )

    st.caption(
        f"OI rank: 1 (highest) to {total:,} (lowest, includes nulls)"
        f"  •  Vol seen: $0M – ${max_vol_musd:,.0f}M"
    )

    return {
        "oi_min": oi_min, "oi_max": oi_max,
        "vol_min_usd": vol_min_musd * 1e6,
        "vol_max_usd": vol_max_musd * 1e6,
        "settles_max": settles_max,
    }


def _apply_filters(df: pd.DataFrame, f: dict) -> pd.DataFrame:
    out = df[
        df["oi_rank"].between(f["oi_min"], f["oi_max"])
        & df["volume_24h_usd"].fillna(0).between(f["vol_min_usd"], f["vol_max_usd"])
    ]
    if f["settles_max"] > 0:
        out = out[out["settles_in_min"].fillna(1e18) <= f["settles_max"]]
    return out


# --------------------------------------------------------------------------
# auto-refreshing fragments
# --------------------------------------------------------------------------

@st.fragment(run_every=15)
def render_header():
    snapshot = _latest_snapshot()
    if snapshot.empty:
        st.warning("No data yet. Run `python collector.py`.")
        return
    last_ts = pd.to_datetime(snapshot["ts_utc"].max(), unit="ms", utc=True)
    age_s = (datetime.now(timezone.utc) - last_ts).total_seconds()
    dot = _freshness_dot(age_s)

    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Venues", snapshot["exchange"].nunique())
    c2.metric("Symbols", snapshot["symbol_canonical"].nunique())
    c3.metric("Pairs", f"{len(snapshot):,}")
    c4.metric(f"{dot} Latest cycle", f"{age_s:.0f}s ago",
              help="Auto-refreshes every 15 s. Green <2 cycles, yellow <4, red older.")
    if c5.button("Refresh now", width="stretch"):
        st.cache_data.clear()
        st.rerun()


@st.fragment(run_every=30)
def render_anomalies():
    snapshot = _latest_snapshot()
    if snapshot.empty:
        return
    snapshot = _add_countdown(snapshot, _now_ms())

    f = {
        "oi_min": st.session_state.get("anom_oi_min", 1),
        "oi_max": st.session_state.get("anom_oi_max", 500),
        "vol_min_usd": st.session_state.get("anom_vol_min", 1.0) * 1e6,
        "vol_max_usd": st.session_state.get("anom_vol_max", 1e9) * 1e6,
        "settles_max": st.session_state.get("anom_settles_max", 0),
    }
    df = _apply_filters(snapshot, f).copy()
    df["apy_pct"] = df["apy_norm"] * 100
    df["vol_musd"] = df["volume_24h_usd"] / 1e6
    df["oi_musd"] = df["open_interest_usd"] / 1e6
    df = (df.assign(_abs=df["apy_pct"].abs())
            .sort_values("_abs", ascending=False).drop("_abs", axis=1))

    # "Forecast (cycle after)" column moved to the rightmost — mostly null.
    cols = ["exchange", "symbol_canonical", "funding_rate", "funding_interval_h",
            "apy_pct", "settles_in_min", "vol_musd", "oi_musd", "oi_rank",
            "predicted_rate"]
    st.caption(f"{len(df):,} pairs match filters")
    st.dataframe(
        df[cols], width="stretch", hide_index=True, height=600,
        column_config=_column_config(),
    )


@st.fragment(run_every=30)
def render_spreads():
    snapshot = _latest_snapshot()
    if snapshot.empty:
        return
    snapshot = _add_countdown(snapshot, _now_ms())

    f = {
        "oi_min": st.session_state.get("spread_oi_min", 1),
        "oi_max": st.session_state.get("spread_oi_max", 500),
        "vol_min_usd": st.session_state.get("spread_vol_min", 1.0) * 1e6,
        "vol_max_usd": st.session_state.get("spread_vol_max", 1e9) * 1e6,
        "settles_max": st.session_state.get("spread_settles_max", 0),
    }
    min_venues = st.session_state.get("spread_min_venues", 2)
    base = _apply_filters(snapshot, f)

    if base.empty:
        st.info("No symbols match filters.")
        return

    # Aggregate per symbol; pull the venues at high/low APY plus their cycle h.
    grouped = base.groupby("symbol_canonical")
    idx_high = grouped["apy_norm"].idxmax()
    idx_low = grouped["apy_norm"].idxmin()
    agg = grouped.agg(
        n_venues=("exchange", "count"),
        apy_high=("apy_norm", "max"),
        apy_low=("apy_norm", "min"),
        min_vol_usd=("volume_24h_usd", "min"),
        max_vol_usd=("volume_24h_usd", "max"),
        best_leg_oi_rank=("oi_rank", "min"),
        worst_leg_oi_rank=("oi_rank", "max"),
        min_settles=("settles_in_min", "min"),
    ).reset_index()
    agg["venue_short"] = base.loc[idx_high, "exchange"].values
    agg["venue_long"] = base.loc[idx_low, "exchange"].values
    short_h = base.loc[idx_high, "funding_interval_h"].astype(float).values
    long_h = base.loc[idx_low, "funding_interval_h"].astype(float).values
    agg["cycles_h"] = [
        f"{int(s)}" if s == l else f"{int(s)}/{int(l)}"
        for s, l in zip(short_h, long_h)
    ]

    agg = agg[agg["n_venues"] >= min_venues].copy()
    agg["apy_high_pct"] = agg["apy_high"] * 100
    agg["apy_low_pct"] = agg["apy_low"] * 100
    agg["delta_apy_pct"] = agg["apy_high_pct"] - agg["apy_low_pct"]
    agg["min_vol_musd"] = agg["min_vol_usd"] / 1e6
    agg["max_vol_musd"] = agg["max_vol_usd"] / 1e6
    agg["settles_in_min"] = agg["min_settles"]
    agg = agg.sort_values("delta_apy_pct", ascending=False)

    cols = ["symbol_canonical", "n_venues",
            "venue_short", "venue_long",
            "apy_high_pct", "apy_low_pct", "delta_apy_pct",
            "settles_in_min", "cycles_h",
            "min_vol_musd", "max_vol_musd",
            "worst_leg_oi_rank", "best_leg_oi_rank"]
    st.caption(f"{len(agg):,} symbols on ≥{min_venues} venues match filters")
    st.dataframe(
        agg[cols], width="stretch", hide_index=True, height=600,
        column_config=_column_config(),
    )


# --------------------------------------------------------------------------
# Symbol history (no auto-refresh; user-driven)
# --------------------------------------------------------------------------

def render_history(snapshot: pd.DataFrame):
    symbols = sorted(snapshot["symbol_canonical"].unique())
    default_sym = "BTC/USDT:USDT" if "BTC/USDT:USDT" in symbols else symbols[0]

    c_a, c_b = st.columns([2, 1])
    with c_a:
        sym = st.selectbox("Symbol", symbols, index=symbols.index(default_sym),
                           key="hist_symbol")
    with c_b:
        hours = st.number_input(
            "Fetch last N hours", min_value=1, max_value=720, value=24, step=1,
            key="hist_hours",
            help="Server-side cap. Use the chart's range buttons (1h/6h/1d/1w) "
                 "or drag-zoom to focus within the fetched window.",
        )

    venues_avail = sorted(
        snapshot.loc[snapshot["symbol_canonical"] == sym, "exchange"].unique()
    )
    chosen = st.multiselect("Venues", venues_avail, default=venues_avail,
                             key="hist_venues")

    hist = _history(sym, hours)
    if hist.empty or not chosen:
        st.info("No history for this selection yet.")
        return
    hist = hist[hist["exchange"].isin(chosen)].copy()
    hist["ts_utc"] = pd.to_datetime(hist["ts_utc"], unit="ms", utc=True)

    fig = make_subplots(
        rows=2, cols=1, shared_xaxes=True, vertical_spacing=0.08,
        subplot_titles=(
            "APY % — annualized; magnitudes directly comparable across venues",
            "Per-epoch rate (raw) — magnitudes vary by funding interval",
        ),
    )
    palette = {}
    colors = ["#1f77b4", "#ff7f0e", "#2ca02c", "#d62728", "#9467bd",
              "#8c564b", "#e377c2", "#7f7f7f", "#bcbd22", "#17becf",
              "#aec7e8", "#ffbb78", "#98df8a"]
    for i, venue in enumerate(chosen):
        palette[venue] = colors[i % len(colors)]

    for venue in chosen:
        d = hist[hist["exchange"] == venue]
        color = palette[venue]
        fig.add_trace(go.Scatter(
            x=d["ts_utc"], y=d["apy_pct"],
            mode="lines+markers", name=venue, legendgroup=venue,
            line=dict(color=color, width=1.5), marker=dict(size=4),
            hovertemplate=("%{x|%H:%M:%S}<br>" + venue
                           + ": %{y:.1f}%%<extra></extra>"),
        ), row=1, col=1)
        fig.add_trace(go.Scatter(
            x=d["ts_utc"], y=d["funding_rate"],
            mode="lines+markers", name=venue, legendgroup=venue,
            showlegend=False,
            line=dict(color=color, width=1.5), marker=dict(size=4),
            hovertemplate=("%{x|%H:%M:%S}<br>" + venue
                           + ": %{y:.6f}<extra></extra>"),
        ), row=2, col=1)

    # Plotly built-in time-range buttons + drag-zoom on x-axis.
    fig.update_xaxes(
        rangeselector=dict(buttons=[
            dict(count=1,  label="1h",  step="hour", stepmode="backward"),
            dict(count=6,  label="6h",  step="hour", stepmode="backward"),
            dict(count=24, label="1d",  step="hour", stepmode="backward"),
            dict(count=72, label="3d",  step="hour", stepmode="backward"),
            dict(count=7,  label="1w",  step="day",  stepmode="backward"),
            dict(step="all", label="All"),
        ]),
        rangeslider=dict(visible=False),
        row=1, col=1,
    )
    fig.update_yaxes(title_text="APY %", row=1, col=1)
    fig.update_yaxes(title_text="Rate / epoch", row=2, col=1)
    fig.update_layout(
        height=720, hovermode="x unified",
        margin=dict(l=20, r=20, t=80, b=20),
        legend=dict(orientation="h", yanchor="bottom", y=1.06,
                    xanchor="right", x=1),
    )
    st.plotly_chart(fig, width="stretch")
    st.caption(f"{len(hist):,} observations across {len(chosen)} venues")


# --------------------------------------------------------------------------
# main
# --------------------------------------------------------------------------

def main():
    st.set_page_config(page_title="Arb Scanalytics", layout="wide",
                       initial_sidebar_state="collapsed")
    st.title("Arb Scanalytics — Live Radar")

    snapshot = _latest_snapshot()
    if snapshot.empty:
        st.warning("No data yet. Run `python collector.py` (or `--once` for one cycle).")
        return

    render_header()

    tab_anom, tab_spread, tab_hist = st.tabs(
        ["Anomalies", "Cross-venue spreads", "Symbol history"]
    )

    with tab_anom:
        st.markdown("##### Filters")
        _filter_inputs("anom", snapshot)
        st.markdown("##### Top anomalies by |APY|")
        render_anomalies()

    with tab_spread:
        st.markdown("##### Filters")
        _filter_inputs("spread", snapshot)
        st.number_input(
            "Min venues for spread", min_value=2, max_value=13, value=2,
            key="spread_min_venues",
        )
        st.markdown("##### Symbols ranked by cross-venue ΔAPY")
        render_spreads()

    with tab_hist:
        st.markdown("##### Funding history overlaid across venues")
        render_history(snapshot)


if __name__ == "__main__":
    main()
