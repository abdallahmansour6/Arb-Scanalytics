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
    # Compute the offset in Python: hours * 3600 * 1000 overflows DuckDB's
    # INT32 inference at hours >= ~596 (720 was the trigger seen in the
    # wild). Python ints are arbitrary precision, and passing the result
    # as a bound parameter has DuckDB infer INT64.
    offset_ms = int(hours) * 3600 * 1000
    return _db().execute("""
        select ts_utc, exchange, funding_rate, funding_interval_h,
               next_funding_ts, predicted_rate,
               apy_norm * 100 as apy_pct, mark_price, volume_24h_usd
        from f
        where symbol_canonical = ?
          and ts_utc >= (select max(ts_utc) from f) - ?
        order by ts_utc
    """, [symbol, offset_ms]).fetchdf()


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
                                  help="Funding-cycle hours at the short / long legs. "
                                       "Single value if both legs match."),
        "apy_pct":            st.column_config.NumberColumn("APY %", format="%.1f"),
        "settles_in_min":     st.column_config.NumberColumn("Settles in (m)", format="%.0f"),
        "vol_musd":           st.column_config.NumberColumn("Vol $M", format="%.1f"),
        "oi_musd":            st.column_config.NumberColumn("OI $M", format="%.1f"),
        "oi_rank":            st.column_config.NumberColumn("OI rank", format="%d",
                                                            help="1 = highest OI globally"),
        "listings":           st.column_config.NumberColumn(
                                  "Listings", format="%d",
                                  help="Total venues that list this symbol. "
                                       "Filter-independent — does NOT change "
                                       "when you tweak the OI / volume / "
                                       "settlement filters."),
        # Per-leg columns (Spreads tab) — these refer to the exact two
        # venues you would actually trade against (short = APY-high,
        # long = APY-low).
        "venue_short":        st.column_config.TextColumn(
                                  "Short venue",
                                  help="Higher-APY venue. Short here: positive funding => shorts receive."),
        "venue_long":         st.column_config.TextColumn(
                                  "Long venue",
                                  help="Lower-APY venue. Long here: negative funding => longs receive."),
        "short_apy_pct":      st.column_config.NumberColumn("Short APY %", format="%.1f"),
        "long_apy_pct":       st.column_config.NumberColumn("Long APY %", format="%.1f"),
        "short_oi_rank":      st.column_config.NumberColumn("Short OI rank", format="%d"),
        "long_oi_rank":       st.column_config.NumberColumn("Long OI rank", format="%d"),
        "short_vol_musd":     st.column_config.NumberColumn("Short vol $M", format="%.1f"),
        "long_vol_musd":      st.column_config.NumberColumn("Long vol $M", format="%.1f"),
        "short_settles_in":   st.column_config.NumberColumn("Short settles (m)", format="%.0f"),
        "long_settles_in":    st.column_config.NumberColumn("Long settles (m)",  format="%.0f"),
        "delta_apy_pct":      st.column_config.NumberColumn("ΔAPY %", format="%.1f"),
    }
    if extra:
        cfg.update(extra)
    return cfg


def _filter_inputs(prefix: str, snapshot: pd.DataFrame, *,
                    include_min_venues: bool = False) -> None:
    """Render filter widgets in semantic, bordered groups. Values land in
    st.session_state under {prefix}_* keys; fragments read them from there.

    Outer column weights are sized to expected content widths so that
    larger-magnitude inputs (volume in $M, decimals) get more pixels than
    smaller ones (min-venues, 2-digit). Each group is wrapped in a bordered
    container so the eye groups related controls visually."""
    total = len(snapshot)
    max_vol_musd = float(snapshot["volume_24h_usd"].max() or 0) / 1e6
    default_oi_max = min(500, total)
    default_vol_max = float(round(max_vol_musd + 1, 0))

    # Outer weights ≈ relative content widths.
    if include_min_venues:
        weights = [2.0, 2.6, 0.9, 0.6]   # OI · Vol · Settle · Min venues
    else:
        weights = [2.0, 2.6, 0.9]
    groups = st.columns(weights, gap="small")

    with groups[0]:
        with st.container(border=True):
            st.markdown("**OI rank**")
            sub = st.columns(2, gap="small")
            with sub[0]:
                st.number_input(
                    "From", min_value=1, max_value=total, value=1, step=10,
                    key=f"{prefix}_oi_min",
                )
            with sub[1]:
                st.number_input(
                    "To", min_value=1, max_value=total, value=default_oi_max,
                    step=10, key=f"{prefix}_oi_max",
                )
            st.caption(
                f"Rank 1 = highest OI · rank {total:,} = lowest. "
                "Pairs with no OI data are ranked at the bottom."
            )

    with groups[1]:
        with st.container(border=True):
            st.markdown("**24h volume ($M)**")
            sub = st.columns(2, gap="small")
            with sub[0]:
                st.number_input(
                    "Min", min_value=0.0, value=0.5, step=1.0,
                    key=f"{prefix}_vol_min",
                )
            with sub[1]:
                st.number_input(
                    "Max", min_value=0.0, value=default_vol_max, step=10.0,
                    key=f"{prefix}_vol_max",
                )
            st.caption(
                f"Smallest pair in dataset: $0M · "
                f"largest: ${max_vol_musd:,.0f}M"
            )

    with groups[2]:
        with st.container(border=True):
            st.markdown("**Settlement**")
            st.number_input(
                "Within (m, 0 = off)", min_value=0, max_value=720,
                value=0, step=15, key=f"{prefix}_settles_max",
            )

    if include_min_venues:
        with groups[3]:
            with st.container(border=True):
                st.markdown("**Cross-venue**")
                st.number_input(
                    "Min venues", min_value=2, max_value=13, value=2,
                    key=f"{prefix}_min_venues",
                )


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
        "vol_min_usd": st.session_state.get("anom_vol_min", 0.5) * 1e6,
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
        "vol_min_usd": st.session_state.get("spread_vol_min", 0.5) * 1e6,
        "vol_max_usd": st.session_state.get("spread_vol_max", 1e9) * 1e6,
        "settles_max": st.session_state.get("spread_settles_max", 0),
    }
    min_venues = st.session_state.get("spread_min_venues", 2)
    base = _apply_filters(snapshot, f)

    if base.empty:
        st.info("No symbols match filters.")
        return

    # Listings = total venue count per symbol, BEFORE the filter is applied.
    # Stable across filter tweaks; informational only.
    listings = snapshot.groupby("symbol_canonical")["exchange"].count()

    # idx_high / idx_low identify the venue at high/low APY for each symbol —
    # the short and long legs you'd actually fire the trade against. EVERY
    # leg-specific column below comes directly from those two rows, never
    # from a min/max aggregation across other venues.
    grouped = base.groupby("symbol_canonical")
    idx_high = grouped["apy_norm"].idxmax()
    idx_low = grouped["apy_norm"].idxmin()
    short_rows = base.loc[idx_high]
    long_rows = base.loc[idx_low]

    # n_venues_filtered drives the Min venues filter; not displayed.
    agg = grouped.agg(
        n_venues_filtered=("exchange", "count"),
    ).reset_index()
    agg["listings"]         = agg["symbol_canonical"].map(listings)
    agg["venue_short"]       = short_rows["exchange"].values
    agg["short_apy_pct"]     = short_rows["apy_norm"].values * 100
    agg["short_oi_rank"]     = short_rows["oi_rank"].values
    agg["short_vol_musd"]    = short_rows["volume_24h_usd"].values / 1e6
    agg["short_settles_in"]  = short_rows["settles_in_min"].values
    agg["venue_long"]        = long_rows["exchange"].values
    agg["long_apy_pct"]      = long_rows["apy_norm"].values * 100
    agg["long_oi_rank"]      = long_rows["oi_rank"].values
    agg["long_vol_musd"]     = long_rows["volume_24h_usd"].values / 1e6
    agg["long_settles_in"]   = long_rows["settles_in_min"].values

    short_h = short_rows["funding_interval_h"].astype(float).values
    long_h = long_rows["funding_interval_h"].astype(float).values
    agg["cycles_h"] = [
        f"{int(s)}" if s == l else f"{int(s)}/{int(l)}"
        for s, l in zip(short_h, long_h)
    ]

    agg = agg[agg["n_venues_filtered"] >= min_venues].copy()
    agg["delta_apy_pct"] = agg["short_apy_pct"] - agg["long_apy_pct"]
    agg = agg.sort_values("delta_apy_pct", ascending=False)

    cols = [
        "symbol_canonical", "listings",
        "delta_apy_pct", "cycles_h",
        "venue_short", "short_apy_pct", "short_oi_rank",
        "short_vol_musd", "short_settles_in",
        "venue_long",  "long_apy_pct",  "long_oi_rank",
        "long_vol_musd",  "long_settles_in",
    ]
    st.caption(f"{len(agg):,} symbols match filters")
    st.dataframe(
        agg[cols], width="stretch", hide_index=True, height=600,
        column_config=_column_config(),
    )


# --------------------------------------------------------------------------
# Symbol history (no auto-refresh; user-driven)
# --------------------------------------------------------------------------

_PALETTE = ["#1f77b4", "#ff7f0e", "#2ca02c", "#d62728", "#9467bd",
            "#8c564b", "#e377c2", "#7f7f7f", "#bcbd22", "#17becf",
            "#aec7e8", "#ffbb78", "#98df8a"]


def _color_for(venue: str, all_venues: list[str]) -> str:
    """Stable color per venue across panels, derived from sorted-venue index."""
    return _PALETTE[sorted(all_venues).index(venue) % len(_PALETTE)]


_RANGE_BUTTONS = dict(buttons=[
    dict(count=1,  label="1h",  step="hour", stepmode="backward"),
    dict(count=6,  label="6h",  step="hour", stepmode="backward"),
    dict(count=24, label="1d",  step="hour", stepmode="backward"),
    dict(count=72, label="3d",  step="hour", stepmode="backward"),
    dict(count=7,  label="1w",  step="day",  stepmode="backward"),
    dict(step="all", label="All"),
])


def _venue_timeframe_row(prefix: str, venues_avail: list[str], *,
                          default_venues: list[str], default_hours: int = 24
                          ) -> tuple[list[str], int]:
    cols = st.columns([3, 1])
    chosen = cols[0].multiselect(
        "Venues", venues_avail, default=default_venues, key=f"{prefix}_venues",
    )
    hours = cols[1].number_input(
        "Fetch last N hours", min_value=1, value=default_hours,
        step=1, key=f"{prefix}_hours",
        help=(
            "Number of hours of history to pull from disk. Bounded only by "
            "how long the collector has been running. Larger windows mean "
            "more points for Plotly to render — interaction stays smooth "
            "up to ~100k total points (≈ 720h on the all-venue chart, more "
            "headroom on the per-venue chart since each panel sees fewer "
            "points). Once fetched, use the chart's range buttons "
            "(1h/6h/1d/3d/1w/All) or drag-zoom to focus."
        ),
    )
    return chosen, hours


def _render_chart_apy(sym: str, chosen: list[str], hours: int,
                      all_venues: list[str]):
    """Chart 1: live drift of upcoming-epoch rate, annualized as APY %.
    Cross-venue magnitudes are directly comparable. Each polled cycle is
    a sample point; step-changes mark settlement boundaries where the
    rate resets for the new upcoming cycle."""
    if not chosen:
        st.info("Select at least one venue.")
        return
    hist = _history(sym, hours)
    hist = hist[hist["exchange"].isin(chosen)].copy()
    if hist.empty:
        st.info("No history yet for this selection.")
        return
    hist["ts_utc"] = pd.to_datetime(hist["ts_utc"], unit="ms", utc=True)

    fig = go.Figure()
    for venue in chosen:
        d = hist[hist["exchange"] == venue].sort_values("ts_utc")
        if d.empty:
            continue
        fig.add_trace(go.Scatter(
            x=d["ts_utc"], y=d["apy_pct"],
            mode="lines+markers", name=venue,
            line=dict(color=_color_for(venue, all_venues), width=1.5),
            marker=dict(size=4),
            # x-axis label is already shown in the unified hover header; the
            # template only needs the venue/value row.
            hovertemplate=venue + ": %{y:.1f}%%<extra></extra>",
        ))
    fig.update_xaxes(rangeselector=_RANGE_BUTTONS,
                     rangeslider=dict(visible=False))
    fig.update_yaxes(title_text="APY %")
    fig.update_layout(
        height=480, hovermode="x unified",
        margin=dict(l=20, r=20, t=40, b=20),
        legend=dict(orientation="h", yanchor="bottom", y=1.02,
                    xanchor="right", x=1),
    )
    st.plotly_chart(fig, width="stretch")
    st.caption(f"{len(hist):,} observations across {hist['exchange'].nunique()} venues")


def _render_chart_per_venue(sym: str, chosen: list[str], hours: int,
                             all_venues: list[str]):
    """Chart 2: per-venue raw rate as small multiples.

    One row per venue, shared time axis, independent y-axis per venue
    (auto-scaled to that venue's range). Vertical dashed lines mark each
    venue's settlement boundaries — the moments where the live upcoming
    rate finalizes and a new cycle begins. Volatility within each panel
    shows as line jaggedness; cycle-to-cycle resets show as step-changes
    crossing the boundaries. σ and range annotations give numeric volatility
    per venue. Magnitudes are NOT directly comparable across venues with
    different cycle intervals (a 1h-cycle rate is naturally ~8× smaller
    than an 8h-cycle rate for the same APY)."""
    if not chosen:
        st.info("Select at least one venue.")
        return
    hist = _history(sym, hours)
    hist = hist[hist["exchange"].isin(chosen)].copy()
    if hist.empty:
        st.info("No history yet for this selection.")
        return
    hist["ts_utc"] = pd.to_datetime(hist["ts_utc"], unit="ms", utc=True)
    visible = [v for v in chosen if v in hist["exchange"].unique()]
    if not visible:
        st.info("No data for selected venues in the chosen window.")
        return

    fig = make_subplots(
        rows=len(visible), cols=1, shared_xaxes=True,
        vertical_spacing=0.04,
        subplot_titles=tuple(visible),
    )

    for i, venue in enumerate(visible, start=1):
        d = hist[hist["exchange"] == venue].sort_values("ts_utc")
        if d.empty:
            continue
        color = _color_for(venue, all_venues)
        cycle_h = int(d["funding_interval_h"].iloc[-1] or 0)

        fig.add_trace(go.Scatter(
            x=d["ts_utc"], y=d["funding_rate"],
            mode="lines+markers", name=venue, showlegend=False,
            line=dict(color=color, width=1.5), marker=dict(size=3),
            # x-axis label already shown in unified hover header. ":.4%" tells
            # plotly's d3-format to multiply by 100 and append "%", so a raw
            # rate of -0.000034 renders as "-0.0034%".
            hovertemplate=venue + ": %{y:.4%}<extra></extra>",
        ), row=i, col=1)

        # Boundary vlines — one per unique next_funding_ts in the window.
        boundaries = (
            pd.to_datetime(d["next_funding_ts"], unit="ms", utc=True)
              .dropna().drop_duplicates().sort_values()
        )
        for b in boundaries:
            fig.add_vline(x=b, line_dash="dot", line_color="gray",
                          opacity=0.45, row=i, col=1)

        # Per-panel volatility annotation (top-right of each panel)
        stdev = float(d["funding_rate"].std() or 0.0)
        rng = float(d["funding_rate"].max() - d["funding_rate"].min())
        fig.add_annotation(
            text=f"cycle: {cycle_h}h  ·  σ = {stdev:.2e}  ·  range = {rng:.2e}",
            xref=f"x{i if i > 1 else ''} domain",
            yref=f"y{i if i > 1 else ''} domain",
            x=0.99, y=0.97, xanchor="right", yanchor="top",
            showarrow=False,
            font=dict(size=10, color="gray"),
        )

        fig.update_yaxes(title_text="Rate", row=i, col=1, automargin=True)

    # Range selector / zoom on top panel (synced to all via shared_xaxes).
    fig.update_xaxes(rangeselector=_RANGE_BUTTONS,
                     rangeslider=dict(visible=False), row=1, col=1)
    fig.update_layout(
        height=max(180 * len(visible), 320),
        hovermode="x unified",
        margin=dict(l=20, r=20, t=60, b=20),
    )
    st.plotly_chart(fig, width="stretch")


def render_history(snapshot: pd.DataFrame):
    symbols = sorted(snapshot["symbol_canonical"].unique())
    default_sym = "BTC/USDT:USDT" if "BTC/USDT:USDT" in symbols else symbols[0]
    sym = st.selectbox("Symbol", symbols, index=symbols.index(default_sym),
                       key="hist_symbol")
    venues_avail = sorted(
        snapshot.loc[snapshot["symbol_canonical"] == sym, "exchange"].unique()
    )

    st.markdown("---")
    st.markdown("#### APY % — live upcoming-rate, annualized")
    st.caption(
        "Each polling cycle (≈60 s) records the *upcoming-boundary* funding "
        "rate for this symbol on each venue. We plot that rate over time, "
        "annualized as APY %, regardless of where the epoch boundaries fall. "
        "Magnitudes are directly comparable across venues. Step-changes "
        "occur at each venue's settlement boundary where the rate resets "
        "for the new upcoming cycle."
    )
    chosen1, hours1 = _venue_timeframe_row(
        "chart_apy", venues_avail, default_venues=venues_avail,
    )
    _render_chart_apy(sym, chosen1, hours1, venues_avail)

    st.markdown("---")
    st.markdown("#### Per-venue raw rate — intra-cycle drift, boundaries marked")
    st.caption(
        "One panel per venue, shared time axis, independent y-axis. Vertical "
        "dashed lines mark each venue's settlement boundaries. Read each "
        "panel for that venue's intra-cycle drift and volatility relative "
        "to itself; magnitudes are NOT directly comparable across venues "
        "(a 1h-cycle rate is naturally ~8× smaller than an 8h-cycle rate "
        "for the same APY). σ (stdev) and range annotations summarize "
        "per-venue volatility within the visible window."
    )
    chosen2, hours2 = _venue_timeframe_row(
        "chart_raw", venues_avail,
        default_venues=venues_avail[:4] if len(venues_avail) > 4 else venues_avail,
    )
    _render_chart_per_venue(sym, chosen2, hours2, venues_avail)


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
        _filter_inputs("spread", snapshot, include_min_venues=True)
        st.markdown("##### Symbols ranked by cross-venue ΔAPY")
        render_spreads()

    with tab_hist:
        st.markdown("##### Funding history overlaid across venues")
        render_history(snapshot)


if __name__ == "__main__":
    main()
