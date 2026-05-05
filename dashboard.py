"""
Live radar dashboard. Streamlit + Plotly + DuckDB over the collector's
Parquet partitions.

Run:
    streamlit run dashboard.py

VS Code Remote-SSH auto-detects the listening port (default 8501) and
surfaces a 'Open in browser' toast that opens it through the SSH tunnel.

Refresh mechanics:
  - Header (top metrics + freshness dot) auto-reruns every 10 s — small,
    cheap visual update so the freshness indicator feels alive.
  - Tables (anomalies, spreads) auto-rerun every 30 s via st.fragment,
    aligned with the collector's 30 s polling cadence — refreshing
    faster just re-renders the same data and dims the table for
    nothing.
  - Filter widgets are OUTSIDE the fragments — typing in a number_input
    doesn't get clobbered by an auto-rerun.
  - `_latest_snapshot()` is @st.cache_data(ttl=30) — matches the table
    refresh, so each fragment run does at most one parquet read.
  - History tab is gated behind an explicit click (see render_history).
    Two reasons: (1) Streamlit evaluates every tab body on every full
    rerun even when hidden, so the chart-build cost would otherwise
    pile onto unrelated reruns (filter changes on other tabs);
    (2) it matches the section's documented intent — user-driven, not
    auto-refreshing.
  - End-to-end disk-write → display latency: ~30 s typical.

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

import numpy as np
import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from plotly.subplots import make_subplots

import store

# Field-notes: VS Code Remote-SSH port-forward probes generate
# 'Invalid HTTP request received' warnings from tornado. Silence; benign.
logging.getLogger("tornado.general").setLevel(logging.ERROR)

log = logging.getLogger("dashboard")


# --------------------------------------------------------------------------
# data access — Streamlit-cached wrappers around store.py primitives
# --------------------------------------------------------------------------
#
# `store.py` owns file-naming conventions, the file enumeration walk, and
# the compaction-race retry pattern (shared with compact.py and any
# future non-Streamlit consumer). The wrappers below add Streamlit's
# `@st.cache_data` so fragment-driven refreshes hit a process-local cache
# instead of re-querying the parquet store on every fragment tick.


@st.cache_data(ttl=30)
def _latest_snapshot() -> pd.DataFrame:
    """One row per (exchange, symbol_canonical) — most recent observation,
    with oi_rank descending by open_interest_usd.

    Rows whose venue does not publish OI (notably all of XT.COM, plus
    per-cycle fan-out failures on BINANCE/BINGX) get oi_rank = NULL —
    they're unranked, not artificially low-ranked. Coalescing NULL → 0
    would silently bury them at the rank tail and let any narrow OI band
    in the dashboard mute them entirely; with NULL they fall through the
    rank filter and get judged on volume / APY instead."""
    # repr() on a list[str] yields a SQL list literal — file paths come
    # from FUNDING_DIR.glob (filesystem-controlled, never user input), so
    # direct embedding is safe.
    def query(db, files):
        return db.sql(f"""
            with latest as (
                select * from read_parquet({files!r}, union_by_name=true)
                qualify row_number() over (partition by exchange, symbol_canonical
                                           order by ts_utc desc) = 1
            )
            select *,
                   case when open_interest_usd is null then null
                        else row_number() over (
                            order by open_interest_usd desc nulls last, exchange
                        )
                   end as oi_rank
            from latest
        """).df()

    return store.read_with_retry(
        store.latest_files, query, retry_label="_latest_snapshot"
    )


@st.cache_data(ttl=60)
def _history(symbol: str, hours: int) -> pd.DataFrame:
    """Time-windowed history for a single symbol across all venues.

    Longer ttl than the snapshot cache: history is user-driven, so we'd
    rather absorb a few unrelated reruns (filter changes on other tabs
    that re-evaluate the History tab body) into one cache-hit re-render
    than re-fetch on each."""
    # Compute the offset in Python: hours * 3600 * 1000 overflows DuckDB's
    # INT32 inference at hours >= ~596 (720 was the trigger seen in the
    # wild). Python ints are arbitrary precision, and passing the result
    # as a bound parameter has DuckDB infer INT64.
    offset_ms = int(hours) * 3600 * 1000

    def query(db, files):
        # Register the narrow file set as a temp view so the WHERE clause's
        # `(select max(ts_utc) from f)` subquery has something to reference.
        db.sql(
            f"create temp view f as "
            f"select * from read_parquet({files!r}, union_by_name=true)"
        )
        return db.execute(
            """
            select ts_utc, exchange, funding_rate, funding_interval_h,
                   next_funding_ts, predicted_rate,
                   apy_norm * 100 as apy_pct, mark_price, volume_24h_usd
            from f
            where symbol_canonical = ?
              and ts_utc >= (select max(ts_utc) from f) - ?
            order by ts_utc
        """,
            [symbol, offset_ms],
        ).fetchdf()

    return store.read_with_retry(
        lambda: store.history_files(hours), query, retry_label="_history"
    )


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
    """🟢 fresh (within ~2 cycles at 30 s cadence), 🟡 missed one, 🔴 stale."""
    if age_s < 60:
        return "🟢"
    if age_s < 120:
        return "🟡"
    return "🔴"


def _column_config(extra: dict | None = None) -> dict:
    cfg = {
        "exchange": st.column_config.TextColumn("Exchange"),
        "symbol_canonical": st.column_config.TextColumn("Symbol"),
        "base_coin": st.column_config.TextColumn(
            "Base",
            help="Underlying token, with multiplier prefix "
            "(1000/1M/1000000) stripped. Used as the "
            "cross-venue grouping key in the spreads "
            "view, since the same coin can appear under "
            "different contract-size multipliers across "
            "venues (e.g. CHEEMS / 1000CHEEMS / "
            "1000000CHEEMS are all the same underlying).",
        ),
        "short_symbol": st.column_config.TextColumn(
            "Short symbol",
            help="The exact venue-specific contract you'd "
            "fire on the short leg (matches the venue's "
            "own listing, including any multiplier prefix).",
        ),
        "long_symbol": st.column_config.TextColumn(
            "Long symbol",
            help="The exact venue-specific contract you'd " "fire on the long leg.",
        ),
        "funding_rate": st.column_config.NumberColumn(
            "Rate (next epoch)", format="%.6f"
        ),
        "predicted_rate": st.column_config.NumberColumn(
            "Forecast (cycle after)", format="%.6f"
        ),
        "funding_interval_h": st.column_config.NumberColumn("Cycle h", format="%.0f"),
        "cycles_h": st.column_config.TextColumn(
            "Cycles h",
            help="Funding-cycle hours at the short / long legs. "
            "Single value if both legs match.",
        ),
        "apy_pct": st.column_config.NumberColumn("APY %", format="%.1f"),
        "settles_in_min": st.column_config.NumberColumn(
            "Settles in (m)", format="%.0f"
        ),
        "vol_musd": st.column_config.NumberColumn("Vol $M", format="%.1f"),
        "oi_musd": st.column_config.NumberColumn("OI $M", format="%.1f"),
        "oi_rank": st.column_config.NumberColumn(
            "OI rank",
            format="%d",
            help="1 = highest OI globally. Empty = venue does not "
            "publish OI for this pair (unranked).",
        ),
        "listings": st.column_config.NumberColumn(
            "Listings",
            format="%d",
            help="Distinct venues that list this base_coin "
            "in any multiplier variant. "
            "Filter-independent — does NOT change "
            "when you tweak the OI / volume / "
            "settlement filters OR the venues-in-scope "
            "selection. So a base_coin can show 8 "
            "listings even when only 2 are in scope.",
        ),
        # Per-leg columns (Spreads tab) — these refer to the exact two
        # venues you would actually trade against (short = APY-high,
        # long = APY-low).
        "venue_short": st.column_config.TextColumn(
            "Short venue",
            help="Higher-APY venue. Short here: positive funding => shorts receive.",
        ),
        "venue_long": st.column_config.TextColumn(
            "Long venue",
            help="Lower-APY venue. Long here: negative funding => longs receive.",
        ),
        "short_apy_pct": st.column_config.NumberColumn("Short APY %", format="%.1f"),
        "long_apy_pct": st.column_config.NumberColumn("Long APY %", format="%.1f"),
        "short_oi_rank": st.column_config.NumberColumn("Short OI rank", format="%d"),
        "long_oi_rank": st.column_config.NumberColumn("Long OI rank", format="%d"),
        "short_vol_musd": st.column_config.NumberColumn("Short vol $M", format="%.1f"),
        "long_vol_musd": st.column_config.NumberColumn("Long vol $M", format="%.1f"),
        "short_settles_in": st.column_config.NumberColumn(
            "Short settles (m)", format="%.0f"
        ),
        "long_settles_in": st.column_config.NumberColumn(
            "Long settles (m)", format="%.0f"
        ),
        "delta_apy_pct": st.column_config.NumberColumn("ΔAPY %", format="%.1f"),
        "entry_basis_bps": st.column_config.NumberColumn(
            "Entry basis bps",
            format="%.1f",
            help="Estimated entry basis at current snapshot prices: "
            "(short_price − long_price) / mid × 10000, with each "
            "leg's price = coalesce(mark_price, last_price). "
            "Engine-convention sign (basis = received − paid): "
            "positive = credit (you collect entering), "
            "negative = cost (you pay). Mirrors --min-entry-basis-bps "
            "directly — a row showing −15 means entering would cost "
            "~15 bps; a floor of −25 still admits it. "
            "Computed from mid-style prices, not L2 VWAP — "
            "systematically ~Σ half-spreads optimistic vs. realized basis "
            "(sub-bp on liquid pairs, 10–50 bps on small-caps).",
        ),
    }
    if extra:
        cfg.update(extra)
    return cfg


def _filter_inputs(
    prefix: str, snapshot: pd.DataFrame, *, include_spread_filters: bool = False
) -> None:
    """Render filter widgets in a single row of bordered groups. Values
    land in st.session_state under {prefix}_* keys; fragments read them
    from there.

    Anomalies tab gets snapshot-level filters only (OI rank, volume).
    Spreads tab additionally renders post-aggregation filters that
    operate on the cross-venue agg (min venues, min ΔAPY, entry basis
    range). Per-tab values are isolated by the {prefix} keys — editing a
    filter in one tab never touches the other.

    Column weights are sized to expected content widths so range-style
    inputs (two number_inputs side-by-side) get more pixels than single-
    input filters."""
    # max() guard for the degenerate case where every row is NULL-OI; the
    # number_input bounds would otherwise be invalid (max < min).
    ranked_total = max(1, int(snapshot["oi_rank"].notna().sum()))
    max_vol_musd = float(snapshot["volume_24h_usd"].max() or 0) / 1e6
    default_oi_max = min(6000, ranked_total)
    default_vol_max = float(round(max_vol_musd + 1, 0))

    # Spreads order: snapshot filters (OI, Vol) → trade-decision filters
    # (Basis, ΔAPY) → infrastructure filter (Cross-venue, last because the
    # operator rarely changes it).
    if include_spread_filters:
        weights = [2.0, 2.6, 2.0, 0.8, 0.6]
    else:
        weights = [2.0, 2.6]
    cols = st.columns(weights, gap="small")

    with cols[0]:
        with st.container(border=True):
            st.markdown("**OI rank**")
            sub = st.columns(2, gap="small")
            with sub[0]:
                st.number_input(
                    "From",
                    min_value=1,
                    max_value=ranked_total,
                    value=1,
                    step=10,
                    key=f"{prefix}_oi_min",
                )
            with sub[1]:
                st.number_input(
                    "To",
                    min_value=1,
                    max_value=ranked_total,
                    value=default_oi_max,
                    step=10,
                    key=f"{prefix}_oi_max",
                )
            st.caption(
                f"Rank 1 = highest OI · rank {ranked_total:,} = lowest. "
                "Pairs without OI data (XT.COM, plus per-cycle misses) are "
                "unranked and pass this filter regardless of the band — they "
                "get judged by the volume filter instead."
            )

    with cols[1]:
        with st.container(border=True):
            st.markdown("**24h volume ($M)**")
            sub = st.columns(2, gap="small")
            with sub[0]:
                st.number_input(
                    "Min",
                    min_value=0.0,
                    value=0.5,
                    step=1.0,
                    key=f"{prefix}_vol_min",
                )
            with sub[1]:
                st.number_input(
                    "Max",
                    min_value=0.0,
                    value=default_vol_max,
                    step=10.0,
                    key=f"{prefix}_vol_max",
                )
            st.caption(
                f"Smallest pair in dataset: $0M · " f"largest: ${max_vol_musd:,.0f}M"
            )

    if not include_spread_filters:
        return

    with cols[2]:
        with st.container(border=True):
            st.markdown("**Entry basis bps**")
            sub = st.columns(2, gap="small")
            with sub[0]:
                st.number_input(
                    "Min",
                    value=-1000.0,
                    step=10.0,
                    key=f"{prefix}_basis_min",
                )
            with sub[1]:
                st.number_input(
                    "Max",
                    value=1000.0,
                    step=10.0,
                    key=f"{prefix}_basis_max",
                )
            st.caption(
                "Engine sign convention: + = credit, − = cost on entry. "
                "Pairs with NaN basis (missing leg prices, or pre-fix data "
                "with NULL base_multiplier) pass this filter unconditionally."
            )

    with cols[3]:
        with st.container(border=True):
            st.markdown("**Min ΔAPY %**")
            st.number_input(
                "≥",
                value=200.0,
                step=10.0,
                key=f"{prefix}_min_delta_apy",
                help="Drop pairs with ΔAPY below this. 0 = off.",
            )

    with cols[4]:
        with st.container(border=True):
            st.markdown("**Cross-venue**")
            st.number_input(
                "Min venues",
                min_value=2,
                max_value=13,
                value=2,
                key=f"{prefix}_min_venues",
                help=(
                    "Minimum number of in-scope venues (passing the "
                    "OI / volume / basis filters above) that must list "
                    "this base_coin for it to appear. Reducing the "
                    "venues-in-scope multiselect shrinks this count "
                    "for every pair."
                ),
            )


def _apply_filters(df: pd.DataFrame, f: dict) -> pd.DataFrame:
    # NULL-OI rows are unranked (see _latest_snapshot); they pass the rank
    # filter unconditionally so a narrow band can't silently mute venues
    # that don't expose OI. The volume filter still gets its say — that's
    # the safety net for the candidate's actual tradeability.
    return df[
        (df["oi_rank"].isna() | df["oi_rank"].between(f["oi_min"], f["oi_max"]))
        & df["volume_24h_usd"].fillna(0).between(f["vol_min_usd"], f["vol_max_usd"])
    ]


def _venue_scope_input(prefix: str, snapshot: pd.DataFrame) -> list[str]:
    """Universe-scope multiselect — semantically distinct from the row-level
    filters in `_filter_inputs()`. Defines which venues this tab's analytics
    even *consider*; applied BEFORE per-row filters and (critically, on
    Spreads) BEFORE the base_coin groupby that picks each pair's short/long
    legs.

    Why before-groupby on Spreads: idxmax/idxmin pick from `base`, so a
    venue dropped here is also dropped from leg-pair selection — which is
    the point. Doing this post-groupby would let idxmax/idxmin pick an
    excluded venue and then hide the entire row, silently dropping spreads
    where two of your in-scope venues had a usable ΔAPY for the same
    base_coin.

    State persists via `{prefix}_venues` session_state, paralleling the
    existing `{prefix}_*` per-tab keys. Default = every venue observed in
    the current snapshot, so the list auto-grows when the collector picks
    up a new venue. Stale entries (a venue that dropped out of the
    snapshot since last render) are pruned defensively to avoid Streamlit
    crashing the multiselect with 'Default value is not in the options'."""
    venues_avail = sorted(snapshot["exchange"].dropna().unique())
    key = f"{prefix}_venues"
    if key in st.session_state:
        st.session_state[key] = [
            v for v in st.session_state[key] if v in venues_avail
        ]

    st.markdown("##### Venues in scope")
    chosen = st.multiselect(
        "Venues",
        venues_avail,
        default=venues_avail,
        key=key,
        label_visibility="collapsed",
        help=(
            "Universe scope, applied BEFORE the row filters below. On "
            "the Spreads tab this also runs BEFORE the base_coin "
            "groupby — so each pair's short/long legs are picked from "
            "the venues you keep here, not globally with hide-after. "
            "The 'Listings' column still reports the global venue "
            "count (unchanged by this selection)."
        ),
    )
    st.caption(
        f"{len(chosen)}/{len(venues_avail)} venues in scope. Excluded "
        "venues are dropped from the snapshot before any analytics on "
        "this tab — including, on Spreads, the short/long leg-pair "
        "selection."
    )
    return chosen


# --------------------------------------------------------------------------
# auto-refreshing fragments
# --------------------------------------------------------------------------


@st.fragment(run_every=10)
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
    c4.metric(
        f"{dot} Latest cycle",
        f"{age_s:.0f}s ago",
        help="Auto-refreshes every 15 s. Green <2 cycles, yellow <4, red older.",
    )
    if c5.button("Refresh now", width="stretch"):
        st.cache_data.clear()
        st.rerun()


@st.fragment(run_every=30)
def render_anomalies():
    # Filter widgets live INSIDE the fragment on purpose: a widget change
    # inside a fragment triggers a *partial* rerun of just this fragment,
    # so editing a filter dims this one table once and leaves the header /
    # other tab untouched. (If they sat outside, every edit would cascade
    # into a full main() rerun and re-execute every fragment on the page.)
    snapshot = _latest_snapshot()
    if snapshot.empty:
        return

    scope = _venue_scope_input("anom", snapshot)
    st.markdown("##### Filters")
    _filter_inputs("anom", snapshot)
    st.markdown("##### Top anomalies by |APY|")

    snapshot = _add_countdown(snapshot, _now_ms())

    f = {
        "oi_min": st.session_state.get("anom_oi_min", 1),
        "oi_max": st.session_state.get("anom_oi_max", 500),
        "vol_min_usd": st.session_state.get("anom_vol_min", 0.5) * 1e6,
        "vol_max_usd": st.session_state.get("anom_vol_max", 1e9) * 1e6,
    }
    # Drop rows without a usable apy_norm — this view ranks by APY, so a
    # row whose APY is NaN (because the venue couldn't supply funding_rate
    # or interval_h for that symbol this cycle) has nothing to contribute.
    # Scope is just another row filter on this tab (no groupby downstream),
    # so order vs the OI/volume filters is irrelevant. Order DOES matter on
    # Spreads — see render_spreads for the asymmetry.
    df = _apply_filters(snapshot, f).dropna(subset=["apy_norm"]).copy()
    df = df[df["exchange"].isin(scope)]
    df["apy_pct"] = df["apy_norm"] * 100
    df["vol_musd"] = df["volume_24h_usd"] / 1e6
    df["oi_musd"] = df["open_interest_usd"] / 1e6
    df = (
        df.assign(_abs=df["apy_pct"].abs())
        .sort_values("_abs", ascending=False)
        .drop("_abs", axis=1)
    )

    # "Forecast (cycle after)" column moved to the rightmost — mostly null.
    cols = [
        "exchange",
        "symbol_canonical",
        "funding_rate",
        "funding_interval_h",
        "apy_pct",
        "settles_in_min",
        "vol_musd",
        "oi_musd",
        "oi_rank",
        "predicted_rate",
    ]
    st.caption(f"{len(df):,} pairs match filters")
    st.dataframe(
        df[cols],
        width="stretch",
        hide_index=True,
        height=600,
        column_config=_column_config(),
    )


# --------------------------------------------------------------------------
# Spreads-tab scatter — the 4D entry point above the ranked table
# --------------------------------------------------------------------------
#
# Each axis answers one of the four sub-questions of trade viability:
#   x  → entry cost           (engine convention: +bps = credit, −bps = cost)
#   y  → yield edge           (ΔAPY %)
#   size  → deployable size   (min-of-legs 24h volume; capacity is bottlenecked
#                              by the worse leg)
#   color → profit-leg timing (settles_in of whichever leg is paying us the
#                              most this epoch — the "max-magnitude payer";
#                              see _profit_leg_settles_in for the rule)
#
# Adding a new dimension is a one-liner: derive the column once on `agg`
# (alongside `min_vol_musd` / `profit_settles_in`), wire it into
# _SPREAD_SCATTER_DIMS, and append to _SPREAD_HOVER_FIELDS if it should also
# appear in the tooltip.

# Single source of truth for axis → column mapping. Swap a dimension here
# without touching the chart code.
_SPREAD_SCATTER_DIMS = {
    "x": "entry_basis_bps",
    "y": "delta_apy_pct",
    "size": "min_vol_musd",
    "color": "profit_settles_in",
}

# Declarative hover content. Each entry is (column, label, fmt) where `fmt`
# is the d3-format suffix used inside `%{customdata[i]fmt}`. Empty fmt ("")
# renders the raw value — works for strings like cycles_h.
_SPREAD_HOVER_FIELDS: list[tuple[str, str, str]] = [
    ("base_coin",        "Base",            ""),
    ("delta_apy_pct",    "ΔAPY %",          ":.1f"),
    ("entry_basis_bps",  "Entry basis bps", ":.1f"),
    ("cycles_h",         "Cycles h",        ""),
    ("venue_short",      "Short venue",     ""),
    ("short_symbol",     "Short symbol",    ""),
    ("short_apy_pct",    "Short APY %",     ":.1f"),
    ("short_vol_musd",   "Short vol $M",    ":.1f"),
    ("short_settles_in", "Short settles m", ":.0f"),
    ("venue_long",       "Long venue",      ""),
    ("long_symbol",      "Long symbol",     ""),
    ("long_apy_pct",     "Long APY %",      ":.1f"),
    ("long_vol_musd",    "Long vol $M",     ":.1f"),
    ("long_settles_in",  "Long settles m",  ":.0f"),
]


def _hover_template(fields: list[tuple[str, str, str]]) -> str:
    """Build a plotly hovertemplate from a list of (col, label, fmt) tuples.
    Each tuple becomes one `<b>label</b>: %{customdata[i]fmt}` line."""
    lines = [
        f"<b>{label}</b>: %{{customdata[{i}]{fmt}}}"
        for i, (_, label, fmt) in enumerate(fields)
    ]
    return "<br>".join(lines) + "<extra></extra>"


def _profit_leg_settles_in(agg: pd.DataFrame) -> pd.Series:
    """Settle-time of the leg paying us the most this epoch.

    Per-leg payment-to-us magnitude:
      * short leg pays us +short_apy when short_apy > 0 (else 0)
      * long  leg pays us −long_apy when long_apy < 0 (else 0)
    Pick the leg with the bigger magnitude. By construction
    short_apy ≥ long_apy (agg is sorted desc by ΔAPY), so net is positive
    and at least one leg is always paying — the rule never picks a "cost"
    leg by mistake.

    Tie (e.g., short_apy = −long_apy exactly): defaults to short. Doesn't
    happen at meaningful precision in practice.
    """
    short_pay = agg["short_apy_pct"].clip(lower=0)
    long_pay = (-agg["long_apy_pct"]).clip(lower=0)
    return agg["short_settles_in"].where(short_pay >= long_pay, agg["long_settles_in"])


def _render_spreads_scatter(agg: pd.DataFrame) -> None:
    """4D scatter over the spreads-tab agg. See module-level dimension
    notes above for the why-of-each-axis."""
    dims = _SPREAD_SCATTER_DIMS
    plot = agg.dropna(subset=[dims["x"], dims["y"]])
    if plot.empty:
        st.info("No points to plot — entry basis or ΔAPY missing on every row.")
        return

    # log1p volume for the marker-area encoding. 24h volume spans 3+ orders
    # of magnitude; linear sizing makes whales dominate and small caps
    # invisible. log1p compresses the range while keeping zero-volume
    # rows valid (rare; mostly XT.COM-on-XT-only base_coins where vol may
    # be unreported on a leg).
    size_raw = plot[dims["size"]].clip(lower=0).fillna(0)
    size_log = np.log1p(size_raw)
    sizeref = (
        (2.0 * float(size_log.max()) / (35**2)) if size_log.max() > 0 else 1.0
    )

    customdata = plot[[col for col, _, _ in _SPREAD_HOVER_FIELDS]].to_numpy()

    fig = go.Figure(
        go.Scatter(
            x=plot[dims["x"]],
            y=plot[dims["y"]],
            mode="markers",
            marker=dict(
                size=size_log,
                sizemode="area",
                sizeref=sizeref,
                sizemin=4,
                color=plot[dims["color"]],
                # Cold→hot semantic: imminent (low minutes) = red ("pay
                # attention now"), distant = blue ("not soon"). Bluered_r
                # has both ends saturated without the RdBu white-midpoint
                # washout that would dim the middle of the range.
                colorscale="Bluered_r",
                colorbar=dict(title="Profit leg<br>settles (m)"),
                line=dict(width=0.5, color="rgba(0,0,0,0.3)"),
            ),
            customdata=customdata,
            hovertemplate=_hover_template(_SPREAD_HOVER_FIELDS),
        )
    )
    # zeroline on x highlights the credit/cost boundary at no extra ink.
    fig.update_xaxes(
        title_text="Entry basis bps  (→ credit, ← cost)",
        zeroline=True,
        zerolinewidth=1,
    )
    fig.update_yaxes(title_text="ΔAPY %")
    fig.update_layout(
        height=520,
        hovermode="closest",
        margin=dict(l=40, r=20, t=20, b=40),
    )
    st.plotly_chart(fig, width="stretch")


@st.fragment(run_every=30)
def render_spreads():
    # See render_anomalies for the rationale behind keeping filters inside
    # the fragment.
    snapshot = _latest_snapshot()
    if snapshot.empty:
        return

    scope = _venue_scope_input("spread", snapshot)
    st.markdown("##### Filters")
    _filter_inputs("spread", snapshot, include_spread_filters=True)

    snapshot = _add_countdown(snapshot, _now_ms())

    f = {
        "oi_min": st.session_state.get("spread_oi_min", 1),
        "oi_max": st.session_state.get("spread_oi_max", 500),
        "vol_min_usd": st.session_state.get("spread_vol_min", 0.5) * 1e6,
        "vol_max_usd": st.session_state.get("spread_vol_max", 1e9) * 1e6,
    }
    min_venues = st.session_state.get("spread_min_venues", 2)
    # Spreads is an APY-ranking view; drop rows whose apy_norm is NaN (the
    # venue couldn't supply funding_rate or interval_h this cycle). They
    # cannot be high or low APY by definition, and including them poisons
    # idxmax/idxmin when a symbol's only matching venues are all-NaN.
    base = _apply_filters(snapshot, f).dropna(subset=["apy_norm"])

    # Apply the venue scope BEFORE the base_coin groupby below. The
    # idxmax/idxmin pick legs from `base`, so any venue dropped here is
    # also dropped from leg-pair selection — which is the point. Doing
    # this post-groupby would let idxmax/idxmin pick an excluded venue
    # and then hide the entire row, silently dropping spreads where two
    # of your in-scope venues had a usable ΔAPY for the same base_coin.
    base = base[base["exchange"].isin(scope)]

    if base.empty:
        st.info("No symbols match filters.")
        return

    # Spreads group by base_coin (multiplier-prefix-stripped base). This
    # collapses listings like '1000CHEEMS', '1MCHEEMS', '1000000CHEEMS', and
    # 'CHEEMS' onto the same logical token — they're the same underlying,
    # priced under different per-contract multipliers. Funding rate is a
    # percentage of contract value, so cross-multiplier ΔAPY is sound.
    # Each leg row still surfaces its actual `symbol_canonical` so the
    # trade routes to the correct venue-specific contract.
    #
    # `listings` is computed from the *unfiltered* snapshot — so it stays
    # both row-filter-independent AND scope-independent: a base_coin can
    # show 'listings = 8' even when only 2 of those 8 are in scope. See
    # _column_config() for the user-visible help text.
    listings = snapshot.groupby("base_coin")["exchange"].nunique()

    # idx_high / idx_low identify the venue at high/low APY for each
    # base_coin — the short and long legs you'd actually fire against.
    # Every leg-specific column comes directly from those two rows.
    grouped = base.groupby("base_coin")
    idx_high = grouped["apy_norm"].idxmax()
    idx_low = grouped["apy_norm"].idxmin()
    short_rows = base.loc[idx_high]
    long_rows = base.loc[idx_low]

    # n_venues_filtered drives the Min venues filter; not displayed.
    agg = grouped.agg(
        n_venues_filtered=("exchange", "count"),
    ).reset_index()
    agg["listings"] = agg["base_coin"].map(listings)
    agg["venue_short"] = short_rows["exchange"].values
    agg["short_symbol"] = short_rows["symbol_canonical"].values
    agg["short_apy_pct"] = short_rows["apy_norm"].values * 100
    agg["short_oi_rank"] = short_rows["oi_rank"].values
    agg["short_vol_musd"] = short_rows["volume_24h_usd"].values / 1e6
    agg["short_settles_in"] = short_rows["settles_in_min"].values
    agg["venue_long"] = long_rows["exchange"].values
    agg["long_symbol"] = long_rows["symbol_canonical"].values
    agg["long_apy_pct"] = long_rows["apy_norm"].values * 100
    agg["long_oi_rank"] = long_rows["oi_rank"].values
    agg["long_vol_musd"] = long_rows["volume_24h_usd"].values / 1e6
    agg["long_settles_in"] = long_rows["settles_in_min"].values

    short_h = short_rows["funding_interval_h"].astype(float).values
    long_h = long_rows["funding_interval_h"].astype(float).values
    agg["cycles_h"] = [
        f"{int(s)}" if s == l else f"{int(s)}/{int(l)}" for s, l in zip(short_h, long_h)
    ]

    # Entry basis bps — engine-convention (basis = received − paid). Coalesce
    # to last_price for venues that don't expose mark (BITMART/HTX/OKX per
    # FIELD_NOTES); NaN propagates if both are missing on either leg.
    #
    # Each leg's price is divided by its base_multiplier first to bring
    # both legs to per-1×-unit. The same underlying coin is listed under
    # different prefix multipliers across venues (`CHEEMS` / `1000CHEEMS`
    # / `1MCHEEMS` / `1000000CHEEMS`); without this normalization, the
    # raw subtraction is meaningless when legs differ in multiplier.
    # NULL multiplier (pre-fix rows or unrecognized prefix) propagates to
    # NaN basis — visible signal rather than silent corruption.
    short_mult = short_rows["base_multiplier"].astype(float).values
    long_mult = long_rows["base_multiplier"].astype(float).values
    short_p = short_rows["mark_price"].fillna(short_rows["last_price"]).values / short_mult
    long_p = long_rows["mark_price"].fillna(long_rows["last_price"]).values / long_mult
    mid = (short_p + long_p) / 2
    agg["entry_basis_bps"] = (short_p - long_p) / mid * 10000

    agg = agg[agg["n_venues_filtered"] >= min_venues].copy()
    agg["delta_apy_pct"] = agg["short_apy_pct"] - agg["long_apy_pct"]
    agg = agg.sort_values("delta_apy_pct", ascending=False)

    # Decision-relevant projections of per-leg data. New scatter / hover
    # dimensions get derived here so the chart and any future analyses
    # (research notebooks, alert logic) read from one named column.
    agg["min_vol_musd"] = agg[["short_vol_musd", "long_vol_musd"]].min(axis=1)
    agg["profit_settles_in"] = _profit_leg_settles_in(agg)

    # Spread-level filters (operate on the agg, not the snapshot — that's
    # why these widgets live in row 2 of the spreads filter bar). Applied
    # before scatter & table so both views share the same filtered set.
    # NaN basis bps (missing leg prices) passes the basis filter
    # unconditionally — same convention as NULL OI rank: a missing-data
    # row shouldn't get muted by a range it can't satisfy.
    min_delta_apy = st.session_state.get("spread_min_delta_apy", 200.0)
    basis_min = st.session_state.get("spread_basis_min", -1000.0)
    basis_max = st.session_state.get("spread_basis_max", 1000.0)
    agg = agg[agg["delta_apy_pct"] >= min_delta_apy]
    agg = agg[
        agg["entry_basis_bps"].isna()
        | agg["entry_basis_bps"].between(basis_min, basis_max)
    ]

    st.markdown("##### Trade-viability scatter")
    st.caption(
        "Each point is a base-coin pair. Upper-right = high yield AND credit "
        "on entry. Size = min(short, long) 24h volume (log-scaled, so cap "
        "spans don't bury small pairs). Color = profit leg's settlement "
        "timing — the leg whose APY pays us the most this epoch. Hover for "
        "full leg detail."
    )
    _render_spreads_scatter(agg)

    st.markdown("##### Symbols ranked by cross-venue ΔAPY")
    cols = [
        "base_coin",
        "listings",
        "delta_apy_pct",
        "entry_basis_bps",
        "cycles_h",
        "venue_short",
        "short_symbol",
        "short_apy_pct",
        "short_oi_rank",
        "short_vol_musd",
        "short_settles_in",
        "venue_long",
        "long_symbol",
        "long_apy_pct",
        "long_oi_rank",
        "long_vol_musd",
        "long_settles_in",
    ]
    st.caption(f"{len(agg):,} symbols match filters")
    st.dataframe(
        agg[cols],
        width="stretch",
        hide_index=True,
        height=600,
        column_config=_column_config(),
    )


# --------------------------------------------------------------------------
# Symbol history (no auto-refresh; user-driven)
# --------------------------------------------------------------------------

_PALETTE = [
    "#1f77b4",
    "#ff7f0e",
    "#2ca02c",
    "#d62728",
    "#9467bd",
    "#8c564b",
    "#e377c2",
    "#7f7f7f",
    "#bcbd22",
    "#17becf",
    "#aec7e8",
    "#ffbb78",
    "#98df8a",
]


def _color_for(venue: str, all_venues: list[str]) -> str:
    """Stable color per venue across panels, derived from sorted-venue index."""
    return _PALETTE[sorted(all_venues).index(venue) % len(_PALETTE)]


def _downsample_for_chart(df: pd.DataFrame, max_points: int = 1000) -> pd.DataFrame:
    """Stride-downsample a sorted-by-time DataFrame to at most ~max_points
    rows. Plotly chokes on multi-thousand-point traces (heavy JSON, slow
    paint, laggy hover/scroll); ~1 point per horizontal pixel is the
    natural ceiling for what the user can perceive anyway. Stride sampling
    preserves cycle-boundary step shapes well enough for the chart-level
    view — the research notebooks read raw cycle data when fidelity matters.

    Boundary vlines and per-panel σ/range annotations are derived from the
    *full* DataFrame upstream of this call so they stay accurate.
    """
    if len(df) <= max_points:
        return df
    stride = (len(df) + max_points - 1) // max_points
    return df.iloc[::stride]


_RANGE_BUTTONS = dict(
    buttons=[
        dict(count=1, label="1h", step="hour", stepmode="backward"),
        dict(count=6, label="6h", step="hour", stepmode="backward"),
        dict(count=24, label="1d", step="hour", stepmode="backward"),
        dict(count=72, label="3d", step="hour", stepmode="backward"),
        dict(count=7, label="1w", step="day", stepmode="backward"),
        dict(step="all", label="All"),
    ]
)


def _venue_timeframe_row(
    prefix: str,
    venues_avail: list[str],
    *,
    default_venues: list[str],
    default_hours: int = 24,
) -> tuple[list[str], int]:
    cols = st.columns([3, 1])
    chosen = cols[0].multiselect(
        "Venues",
        venues_avail,
        default=default_venues,
        key=f"{prefix}_venues",
    )
    hours = cols[1].number_input(
        "Fetch last N hours",
        min_value=1,
        value=default_hours,
        step=1,
        key=f"{prefix}_hours",
        help=(
            "Number of hours of history to pull from disk. Bounded only by "
            "how long the collector has been running. Each trace is "
            "stride-downsampled to ~1000 points before rendering, and "
            "drawn via WebGL (Scattergl), so chart interaction stays "
            "smooth at any window size — fetch time scales with the "
            "underlying parquet read. Once fetched, use the chart's range "
            "buttons (1h/6h/1d/3d/1w/All) or drag-zoom to focus."
        ),
    )
    return chosen, hours


def _render_chart_apy(sym: str, chosen: list[str], hours: int, all_venues: list[str]):
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
    rendered = 0
    for venue in chosen:
        d_full = hist[hist["exchange"] == venue].sort_values("ts_utc")
        if d_full.empty:
            continue
        d = _downsample_for_chart(d_full)
        rendered += len(d)
        fig.add_trace(
            # Scattergl renders to a WebGL canvas instead of SVG — same
            # API, dramatically faster for >1 k points (initial paint,
            # hover, scroll). Markers drop out for dense traces since
            # they're indistinguishable when overlapping pixel-by-pixel.
            go.Scattergl(
                x=d["ts_utc"],
                y=d["apy_pct"],
                mode="lines+markers" if len(d) <= 200 else "lines",
                name=venue,
                line=dict(color=_color_for(venue, all_venues), width=1.5),
                marker=dict(size=4),
                # x-axis label is already shown in the unified hover header; the
                # template only needs the venue/value row.
                hovertemplate=venue + ": %{y:.1f}%%<extra></extra>",
            )
        )
    fig.update_xaxes(rangeselector=_RANGE_BUTTONS, rangeslider=dict(visible=False))
    fig.update_yaxes(title_text="APY %")
    fig.update_layout(
        height=480,
        hovermode="x unified",
        margin=dict(l=20, r=20, t=40, b=20),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
    )
    st.plotly_chart(fig, width="stretch")
    st.caption(
        f"{len(hist):,} observations across {hist['exchange'].nunique()} venues "
        f"· rendered {rendered:,} points after downsampling"
    )


def _render_chart_per_venue(
    sym: str, chosen: list[str], hours: int, all_venues: list[str]
):
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
        rows=len(visible),
        cols=1,
        shared_xaxes=True,
        vertical_spacing=0.04,
        subplot_titles=tuple(visible),
    )

    for i, venue in enumerate(visible, start=1):
        d_full = hist[hist["exchange"] == venue].sort_values("ts_utc")
        if d_full.empty:
            continue
        color = _color_for(venue, all_venues)
        cycle_h = int(d_full["funding_interval_h"].iloc[-1] or 0)

        # Trace from downsampled data; metadata (boundaries, σ, range) is
        # computed from the FULL DataFrame below so accuracy isn't affected.
        d = _downsample_for_chart(d_full)
        fig.add_trace(
            go.Scattergl(
                x=d["ts_utc"],
                y=d["funding_rate"],
                mode="lines+markers" if len(d) <= 200 else "lines",
                name=venue,
                showlegend=False,
                line=dict(color=color, width=1.5),
                marker=dict(size=3),
                # x-axis label already shown in unified hover header. ":.4%" tells
                # plotly's d3-format to multiply by 100 and append "%", so a raw
                # rate of -0.000034 renders as "-0.0034%".
                hovertemplate=venue + ": %{y:.4%}<extra></extra>",
            ),
            row=i,
            col=1,
        )

        # Boundary vlines — one per unique next_funding_ts in the window.
        # Computed from full data so we don't accidentally skip a boundary
        # whose rows happened to fall outside the downsampling stride.
        boundaries = (
            pd.to_datetime(d_full["next_funding_ts"], unit="ms", utc=True)
            .dropna()
            .drop_duplicates()
            .sort_values()
        )
        for b in boundaries:
            fig.add_vline(
                x=b, line_dash="dot", line_color="gray", opacity=0.45, row=i, col=1
            )

        # Per-panel volatility annotation — full-data stats, not downsampled.
        stdev = float(d_full["funding_rate"].std() or 0.0)
        rng = float(d_full["funding_rate"].max() - d_full["funding_rate"].min())
        fig.add_annotation(
            text=f"cycle: {cycle_h}h  ·  σ = {stdev:.2e}  ·  range = {rng:.2e}",
            xref=f"x{i if i > 1 else ''} domain",
            yref=f"y{i if i > 1 else ''} domain",
            x=0.99,
            y=0.97,
            xanchor="right",
            yanchor="top",
            showarrow=False,
            font=dict(size=10, color="gray"),
        )

        fig.update_yaxes(title_text="Rate", row=i, col=1, automargin=True)

    # Range selector / zoom on top panel (synced to all via shared_xaxes).
    fig.update_xaxes(
        rangeselector=_RANGE_BUTTONS, rangeslider=dict(visible=False), row=1, col=1
    )
    fig.update_layout(
        height=max(180 * len(visible), 320),
        hovermode="x unified",
        margin=dict(l=20, r=20, t=60, b=20),
    )
    st.plotly_chart(fig, width="stretch")


def render_history(snapshot: pd.DataFrame):
    symbols = sorted(snapshot["symbol_canonical"].unique())
    default_sym = "BTC/USDT:USDT" if "BTC/USDT:USDT" in symbols else symbols[0]
    sym = st.selectbox(
        "Symbol", symbols, index=symbols.index(default_sym), key="hist_symbol"
    )

    # Lazy gate. Streamlit evaluates every tab body on every full-page rerun,
    # even when the tab isn't visible — so unconditionally rendering the two
    # charts here would put the parquet read + figure build + browser repaint
    # in the critical path of unrelated reruns (e.g., touching a filter on
    # the Anomalies tab). The flag is per-session and persists until a
    # browser refresh; once set, subsequent symbol/window changes re-fetch
    # and re-render in place.
    if not st.session_state.get("hist_loaded"):
        st.caption(
            "History charts are loaded on demand to keep the live radar "
            "tabs snappy across unrelated interactions. Click to load."
        )
        if st.button("Load history charts", type="primary"):
            st.session_state.hist_loaded = True
            st.rerun()
        return

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
        "chart_apy",
        venues_avail,
        default_venues=venues_avail,
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
        "chart_raw",
        venues_avail,
        # Default to 2 venues: chart-2 build cost (subplots + vlines +
        # annotations per panel) scales with panel count, and 2 panels
        # is enough for the typical "compare these two specifically"
        # check. Add more via the multiselect when needed.
        default_venues=venues_avail[:2] if len(venues_avail) > 2 else venues_avail,
    )
    _render_chart_per_venue(sym, chosen2, hours2, venues_avail)


# --------------------------------------------------------------------------
# main
# --------------------------------------------------------------------------


def main():
    st.set_page_config(
        page_title="Arb Scanalytics", layout="wide", initial_sidebar_state="collapsed"
    )
    st.title("Arb Scanalytics — Live Radar")

    snapshot = _latest_snapshot()
    if snapshot.empty:
        st.warning(
            "No data yet. Run `python collector.py` (or `--once` for one cycle)."
        )
        return

    render_header()

    tab_anom, tab_spread, tab_hist = st.tabs(
        ["Anomalies", "Cross-venue spreads", "Symbol history"]
    )

    # Filters + section headers live inside each fragment now (see
    # render_anomalies / render_spreads); editing a filter then re-runs
    # only that fragment instead of cascading into a full main() rerun.
    with tab_anom:
        render_anomalies()

    with tab_spread:
        render_spreads()

    with tab_hist:
        st.markdown("##### Funding history overlaid across venues")
        render_history(snapshot)


if __name__ == "__main__":
    main()
