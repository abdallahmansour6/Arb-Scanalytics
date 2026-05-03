"""
Shared parquet-store primitives — file conventions, enumeration, and
DuckDB read with compaction-race retry.

Streamlit-free by design. Every consumer of the parquet store imports
from here so file-naming conventions and the read-time race-safety
pattern live in exactly one place:

  * `dashboard.py` — Streamlit UI; wraps these in `@st.cache_data` for
    fragment-driven refresh.
  * `compact.py` — the writer-side of the lifecycle; uses the regexes
    and `venue_of_source()` to identify what to consume vs skip.
  * Research notebooks / future workers — drop-in import; no Streamlit
    dependency dragged along.

Lifecycle (mirrors compact.py's docstring; see there for the why):

    per-cycle:  <VENUE>_<ms_ts>.parquet
                  ↓ hourly compaction (cron 5 * * * *)
    hourly:     <VENUE>_hourly_<YYYYMMDDHH>.parquet
                  ↓ daily compaction (cron 30 0 * * *)
    daily:      <VENUE>_daily.parquet
"""

import logging
import re
from collections import defaultdict
from collections.abc import Callable
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import pandas as pd

from config import FUNDING_DIR

log = logging.getLogger("store")


# --------------------------------------------------------------------------
# Filename conventions — single source of truth
# --------------------------------------------------------------------------
#
# Per-cycle is sortable-by-ms (last digit-group is the cycle's ms timestamp).
# Hourly is sortable-by-bucket (last digit-group is YYYYMMDDHH and orders
# correctly as both string and int). Daily is per-venue, no temporal sort
# needed — it's the deepest fallback. The `[A-Z\.]+` venue class matches
# all current venues (BINANCE, GATE.IO, XT.COM, …) and any future
# all-uppercase or dotted-name additions; lowercase or hyphenated venue
# slugs would need a regex update.

CYCLE_RE = re.compile(r"^([A-Z\.]+)_(\d{13})\.parquet$")
HOURLY_RE = re.compile(r"^([A-Z\.]+)_hourly_(\d{10})\.parquet$")
DAILY_RE = re.compile(r"^([A-Z\.]+)_daily\.parquet$")


def venue_of_source(name: str) -> str | None:
    """Venue for a *source* file (per-cycle or hourly).
    Returns None for already-compacted daily files or unrecognized names —
    callers skip those when listing what's available to compact."""
    for pat in (CYCLE_RE, HOURLY_RE):
        m = pat.match(name)
        if m:
            return m.group(1)
    return None


# --------------------------------------------------------------------------
# File enumeration — one directory walk, two view-shaped projections
# --------------------------------------------------------------------------
#
# `classify_files()` is the only directory walk in the read hot path
# (~150 ms for ~10 k files). `latest_files()` and `history_files()` are
# thin projections over its output; future query patterns (e.g.,
# fixed-time-range, per-venue snapshots) compose the same way.


def classify_files() -> tuple[
    dict[str, list[tuple[int, str]]],
    dict[str, list[tuple[int, str]]],
    list[str],
]:
    """Walk FUNDING_DIR once and bucket every `*.parquet` by lifecycle.
    Returns (per_venue_cycle, per_venue_hourly, daily_or_unknown), where
    each per-venue list is `[(ms_anchor, path), ...]`:
      * cycle: ms_anchor is the cycle's filename ms timestamp.
      * hourly: ms_anchor is the bucket's start-of-hour ms (parsed from
        the YYYYMMDDHH suffix), so consumers can compare it to a window
        cutoff in the same units as cycle.

    Paths are normalized to forward slashes for cross-platform DuckDB
    compatibility — Path stringifies with backslashes on Windows, which
    we don't want leaking into a SQL string literal.
    """
    cycle: dict[str, list[tuple[int, str]]] = defaultdict(list)
    hourly: dict[str, list[tuple[int, str]]] = defaultdict(list)
    daily: list[str] = []
    for path in FUNDING_DIR.glob("**/*.parquet"):
        spath = str(path).replace("\\", "/")
        m = CYCLE_RE.search(path.name)
        if m:
            cycle[m.group(1)].append((int(m.group(2)), spath))
            continue
        m = HOURLY_RE.search(path.name)
        if m:
            s = m.group(2)  # "YYYYMMDDHH"
            bucket_ms = int(datetime(
                int(s[:4]), int(s[4:6]), int(s[6:8]), int(s[8:10]),
                tzinfo=timezone.utc,
            ).timestamp() * 1000)
            hourly[m.group(1)].append((bucket_ms, spath))
            continue
        daily.append(spath)
    return cycle, hourly, daily


def latest_files(k_cycle: int = 20, k_hourly: int = 3) -> list[str]:
    """Files needed for a 'latest snapshot per (exchange, symbol)' view.

    For each venue: the `k_cycle` most recent per-cycle files (covers
    ~10 min at K=20, 30 s cadence) plus the `k_hourly` most recent hourly
    compacted files (a few hours of fallback) plus all daily files. A
    pair that's been intermittently dropped from a venue's batch
    responses for up to a few hours still surfaces in the snapshot via
    `qualify rn=1`'s pick of the most recent row across these files.

    Bounded file count: 13 venues × (k_cycle + k_hourly) + ~13 daily ≈
    a few hundred paths max."""
    cycle, hourly, daily = classify_files()
    selected = list(daily)
    for items in hourly.values():
        items.sort(reverse=True)
        selected.extend(p for _, p in items[:k_hourly])
    for items in cycle.values():
        items.sort(reverse=True)
        selected.extend(p for _, p in items[:k_cycle])
    return selected


def history_files(hours: int) -> list[str]:
    """Files needed to cover the last `hours` of history for any symbol.

    Per-cycle files within the window (cutoff = max-filename-ms − hours_ms)
    plus hourly compacted buckets that *intersect* the window plus all
    daily files. Hourly bucket [t, t+1h) intersects the window iff
    `t + 1h > cutoff`. The SQL still applies the exact ts_utc filter on
    top, so any imprecision in the file-set bounds only changes which
    files we read, not the result.

    With hourly compaction running, today's partition holds at most ~1 h
    of un-compacted per-cycle files plus the bucket files for completed
    hours; for a 24 h window we read ~1 h of cycles + 24 hourly + ~13
    daily ≈ a few hundred files."""
    hours_ms = int(hours) * 3600 * 1000
    cycle_by_venue, hourly_by_venue, daily = classify_files()

    cycle_flat = [it for items in cycle_by_venue.values() for it in items]
    hourly_flat = [it for items in hourly_by_venue.values() for it in items]

    if not cycle_flat and not hourly_flat:
        return list(daily)

    # Window anchor = freshest available ts. Prefer a cycle file when
    # present (per-cycle ms is always >= the latest hourly bucket start).
    anchor_ms = (
        max(t for t, _ in cycle_flat) if cycle_flat
        else max(t for t, _ in hourly_flat)
    )
    cutoff_ms = anchor_ms - hours_ms

    selected = [p for t, p in cycle_flat if t >= cutoff_ms]
    selected.extend(p for t, p in hourly_flat if t + 3_600_000 > cutoff_ms)
    selected.extend(daily)
    return selected


# --------------------------------------------------------------------------
# Read with compaction-race retry
# --------------------------------------------------------------------------


def read_with_retry(
    files_fn: Callable[[], list[str]],
    query_fn: Callable[[duckdb.DuckDBPyConnection, list[str]], pd.DataFrame],
    *,
    retry_label: str = "query",
) -> pd.DataFrame:
    """Run `query_fn(db, files)` over a fresh DuckDB connection, with one
    retry on the compaction-race IOException ('Cannot open file ...').

    Why: the hourly compactor's atomic `rename + delete sources` pattern
    can cause a per-cycle file to vanish in the millisecond gap between
    `files_fn()` enumeration and DuckDB's open. The retry re-enumerates
    via a fresh `files_fn()` call and runs against the post-compaction
    file set; the second attempt always sees a stable view.

    Each attempt opens its own connection so a query that died mid-flight
    can never leave shared state in a poisoned spot for the next caller —
    same isolation rationale as not using `@st.cache_resource` for a
    long-lived view in the dashboard.

    Returns an empty DataFrame if `files_fn()` returns an empty list
    (saves opening DuckDB only to query nothing)."""
    for attempt in (0, 1):
        files = files_fn()
        if not files:
            return pd.DataFrame()
        db = duckdb.connect()
        try:
            return query_fn(db, files)
        except duckdb.IOException as e:
            if attempt == 1 or "Cannot open file" not in str(e):
                raise
            log.info("compaction race in %s; retrying", retry_label)
        finally:
            db.close()
    return pd.DataFrame()  # unreachable; keeps type-checker happy
