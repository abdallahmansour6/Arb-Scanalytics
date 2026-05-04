"""
Async collector. One task per venue.

Each task pulls funding/OI/mark/volume for every USDT-linear perp on its
venue, normalizes to the canonical row shape, and appends one Parquet
file to the active partition.

DESIGN RULE — every field has ONE explicit, authoritative source per
venue. There are NO cascading fallbacks. If the authoritative source
is missing for a row, the field is NULL. Never substitute 'last' for
'mark', and never default an interval to 8 h. The per-venue extraction
table is documented in FIELD_NOTES.md.

The single allowed *derivation* is UTC-aligned next_funding_ts — when
a venue exposes the interval but not the timestamp, we round the
current ts up to the next interval boundary. Verified empirically:
all 13 venues align their boundaries to the UTC grid for 8h/4h/1h
cycles. This is a math derivation from authoritative inputs, not a
fake fallback.

Failure of any single venue is isolated and logged; sibling venues
continue. Run with --once for a single cycle (test-drive); without
flags, loops until SIGTERM/SIGINT.
"""

import argparse
import asyncio
import logging
import re
import signal
import time
from collections import Counter
from contextlib import suppress
from datetime import datetime, timezone

import aiohttp
import pyarrow as pa
import pyarrow.parquet as pq

from config import (
    VENUES,
    FUNDING_DIR,
    POLL_INTERVAL_S,
    MARKETS_RELOAD_INTERVAL_S,
    open_client,
)

# ---------------------------------------------------------------------------
# Canonical schema (locked at collector boundary)
# ---------------------------------------------------------------------------
#
# Three funding-rate concepts, two stored:
#   (A) Last-settled — historical, frozen until next boundary. NOT stored.
#   (B) Upcoming     — what will settle at next_funding_ts. Stored as
#                       funding_rate. CCXT's unified fundingRate maps here.
#   (C) Forward forecast — prediction for the cycle AFTER upcoming. Stored
#                          as predicted_rate. Only 4 venues expose it
#                          (COINEX, HTX, KUCOIN, OKX, PHEMEX); NULL elsewhere.
#
# Three price fields, in decreasing precision for basis-bps math:
#   mark_price  — the venue's published mark price (composite spot
#                 reference; what funding/liquidations/PnL use). NULL
#                 where the venue does not publish it (BITMART, HTX,
#                 OKX). Always prefer this for basis math when populated.
#   index_price — the venue's published index price (multi-venue spot
#                 composite). NULL where not exposed (HTX, OKX).
#   last_price  — most recent trade price. UNIVERSALLY populated across
#                 all 13 venues. Diverges from mark by typically a few
#                 basis points in calm markets, wider in fast moves; that
#                 divergence propagates 1:1 into any basis-bps estimate
#                 derived from it. Stored as a separate primitive so
#                 downstream queries can explicitly choose
#                 `coalesce(mark_price, last_price)` and reason about the
#                 precision tradeoff. The execution engine uses real-time
#                 L2 VWAP; these fields are for scanner-level estimation.

SCHEMA = pa.schema(
    [
        ("ts_utc", pa.int64()),  # ms since epoch (UTC)
        ("exchange", pa.string()),
        (
            "symbol_canonical",
            pa.string(),
        ),  # CCXT-unified, venue-truth (e.g. '1000000CHEEMS/USDT:USDT')
        (
            "base_coin",
            pa.string(),
        ),  # multiplier-prefix-stripped base for cross-venue grouping
        # Integer multiplier encoded in the symbol prefix (1000, 1_000_000, …);
        # 1 when no prefix. Cross-venue price comparisons MUST divide each
        # leg's mark/index/last by this — a venue listing CHEEMS as
        # `1000000CHEEMS` reports a price 1e6× another venue's `CHEEMS`,
        # and unnormalized subtraction yields nonsensical basis_bps.
        ("base_multiplier", pa.int32()),
        ("funding_rate", pa.float64()),  # (B) upcoming-boundary rate
        ("funding_interval_h", pa.float32()),  # authoritative; NULL if unobtainable
        ("predicted_rate", pa.float64()),  # (C) cycle-after forecast
        ("next_funding_ts", pa.int64()),  # authoritative or UTC-derived from interval
        ("mark_price", pa.float64()),  # NULL if venue does not publish mark
        ("index_price", pa.float64()),  # NULL if venue does not publish index
        ("last_price", pa.float64()),  # most recent trade; ~universally populated
        ("open_interest_usd", pa.float64()),
        ("volume_24h_usd", pa.float64()),
        ("apy_norm", pa.float64()),  # rate * 8760 / interval_h
    ]
)


log = logging.getLogger("scanner.collector")


# ---------------------------------------------------------------------------
# Small helpers (no fallbacks; coerce-to-None on bad input)
# ---------------------------------------------------------------------------


def _f(v):
    """Defensive float coerce. Empty string and None both become None.
    Reject NaN-likes the same way."""
    if v is None or v == "":
        return None
    try:
        return float(v)
    except (ValueError, TypeError):
        return None


def _is_usdt_linear(market: dict | None) -> bool:
    """USDT-quoted, USDT-settled, linear, swap, not deactivated. Per the
    audit, every active venue offers BTC/USDT:USDT in this exact shape.
    coinex sets active=None instead of True; treat None as pass."""
    if not market or not market.get("swap") or not market.get("linear"):
        return False
    if market.get("active") is False:
        return False
    return market.get("quote") == "USDT" and market.get("settle") == "USDT"


def _canonical_symbol(market: dict) -> str:
    return market["symbol"]


# Multiplier-prefix parser. Different venues list the same underlying
# token under different contract-size multipliers — e.g. CHEEMS-the-meme
# appears on the wire as `CHEEMS` (COINEX/GATE.IO/MEXC), `1000CHEEMS`
# (BINANCE/BINGX/BITMART/KUCOIN/PHEMEX/XT), `1MCHEEMS` (BITGET), and
# `1000000CHEEMS` (BYBIT). All four are the same underlying; the prefix
# is purely a display convention for tokens whose 1× spot price is too
# small to render legibly.
#
# Two derived fields come from the same parse:
#   * `base_coin` (prefix stripped) — cross-venue grouping key. Funding
#     rate is a percentage of contract value and is multiplier-invariant,
#     so cross-multiplier ΔAPY math is sound on this key alone.
#   * `base_multiplier` (prefix as integer; 1 when none) — the unit of
#     the venue's price columns (mark/index/last). Anything comparing
#     prices across venues MUST divide by this first; without it,
#     `1000000CHEEMS@$0.63` looks 1e6× apart from `CHEEMS@$6.3e-7` when
#     they're the same per-1× price.
#
# The lookahead `(?=[A-Za-z])` is what protects coins like `1INCH`
# (digits-then-letters with no zero) from being mis-stripped. Verified
# safe against `1INCH`, `BTC`, `ETH`. Strips: `100`, `1000`, `10000`,
# `100000`, `1000000`, `10000000`, `1K`, `1M` (case-insensitive).
_MULTIPLIER_PREFIX = re.compile(r"^(?:1[KM]|10{2,7})(?=[A-Za-z])", re.IGNORECASE)


def _base_coin(market: dict) -> str:
    """Strip the multiplier prefix from market.base. Returns the base
    unchanged when no prefix matches. Used as the cross-venue grouping
    key in the spreads view."""
    return _MULTIPLIER_PREFIX.sub("", market.get("base", ""))


def _base_multiplier(market: dict) -> int:
    """Integer multiplier encoded in market.base's prefix; 1 when no
    prefix. `1K` → 1000, `1M` → 1_000_000, otherwise the literal int
    (`100`/`1000`/.../`10000000`). Stored alongside `base_coin` so any
    cross-venue price math can normalize venues to per-1× units."""
    raw = market.get("base", "")
    m = _MULTIPLIER_PREFIX.match(raw)
    if not m:
        return 1
    prefix = m.group(0).upper()
    if prefix == "1K":
        return 1_000
    if prefix == "1M":
        return 1_000_000
    return int(prefix)


def _apy_norm(rate, interval_h):
    """rate (per-epoch fraction) × 8760 / interval_h → annualized fraction."""
    if rate is None or not interval_h:
        return None
    return rate * 8760.0 / interval_h


def _utc_next_boundary(ts_ms: int, interval_h: float | None) -> int | None:
    """UTC-aligned next-funding-boundary derivation. Used ONLY when a
    venue exposes the interval but not a per-symbol next-funding
    timestamp. Verified empirically: every venue in this project aligns
    its boundaries to the UTC grid (BTC's nextFundingTimestamp matches
    across venues at any cycle length sampled — 8h/4h/1h)."""
    if not interval_h:
        return None
    interval_ms = int(interval_h * 3600 * 1000)
    return ((ts_ms // interval_ms) + 1) * interval_ms


def _build_row(
    *,
    ts_ms,
    exchange,
    symbol,
    base_coin,
    base_multiplier,
    funding_rate,
    interval_h,
    predicted=None,
    next_ts=None,
    mark=None,
    index=None,
    last=None,
    oi_usd=None,
    vol_usd=None,
) -> dict:
    """Assemble one canonical row. The only derivations are apy_norm
    (from funding_rate × 8760 / interval_h) and the UTC-aligned next_ts
    fallback when the venue does not publish a timestamp directly. Every
    other field is verbatim from the venue's authoritative source.

    `last` is the most-recent-trade price from the venue's ticker; stored
    as `last_price` and intended as a deliberate downstream fallback for
    `mark_price` when running cross-venue basis-bps analytics on venues
    that don't publish a mark. The schema's `mark_price` itself stays
    strictly authoritative — never overwritten by `last`."""
    if not next_ts and interval_h:
        next_ts = _utc_next_boundary(ts_ms, interval_h)
    return {
        "ts_utc": ts_ms,
        "exchange": exchange,
        "symbol_canonical": symbol,
        "base_coin": base_coin,
        "base_multiplier": int(base_multiplier),
        "funding_rate": funding_rate,
        "funding_interval_h": float(interval_h) if interval_h else None,
        "predicted_rate": predicted,
        "next_funding_ts": int(next_ts) if next_ts else None,
        "mark_price": mark,
        "index_price": index,
        "last_price": last,
        "open_interest_usd": oi_usd,
        "volume_24h_usd": vol_usd,
        "apy_norm": _apy_norm(funding_rate, interval_h),
    }


def _write_partition(exchange: str, ts_ms: int, rows: list[dict]):
    if not rows:
        return
    now = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)
    partition = (
        FUNDING_DIR
        / f"year={now.year:04d}"
        / f"month={now.month:02d}"
        / f"day={now.day:02d}"
    )
    partition.mkdir(parents=True, exist_ok=True)
    file = partition / f"{exchange}_{ts_ms}.parquet"
    table = pa.Table.from_pylist(rows, schema=SCHEMA)
    pq.write_table(table, file, compression="snappy")


# ---------------------------------------------------------------------------
# Per-venue collectors. Each function owns its venue's full extraction:
# what to fetch, where to read each field, what the unit conversion is.
# Single source per field. NULL where the source is unavailable.
#
# Fan-out venues (BINANCE, BINGX, XT.COM) make hundreds of per-symbol
# calls per cycle; per-call failures used to be silently swallowed,
# which masked rate-limit incidents and gradual data degradation. Each
# fan-out now counts failures by exception type and emits a structured
# WARNING when any cycle has non-zero failures, so the symptoms surface
# the moment they appear.
# ---------------------------------------------------------------------------


def _log_fanout(
    exchange: str, op: str, attempted: int, failures: "Counter[str]"
) -> None:
    """Emit a single-line structured warning if a cycle's fan-out had
    failures. Silent when everything succeeded. Failure count is
    broken down by exception type so rate-limit pressure is
    distinguishable from network blips or venue-side outages."""
    if not failures:
        return
    total = sum(failures.values())
    breakdown = ", ".join(f"{etype}={n}" for etype, n in failures.most_common())
    log.warning(
        "[%s] %s fan-out: %d/%d failed (%s)", exchange, op, total, attempted, breakdown
    )


# -- BINANCE -----------------------------------------------------------------


async def collect_binance(c, ts_ms):
    """Sources:
    funding_rate     : batch f.unified.fundingRate
    interval_h       : binance fapiPublicGetFundingInfo per-symbol fundingIntervalHours
    next_funding_ts  : batch f.info.nextFundingTime
    mark             : batch f.info.markPrice
    index            : batch f.info.indexPrice
    oi_usd           : per-symbol openInterestAmount × contractSize × mark (fan-out)
    vol_usd          : t.unified.quoteVolume
    predicted        : not exposed → NULL
    """
    # Per-symbol funding-interval map (one extra REST call returning all symbols).
    fi = await c.fapiPublicGetFundingInfo()
    interval_by_sym = {}
    for entry in fi:
        ccxt_id = entry.get("symbol")
        # Map BINANCE id (e.g. "BTCUSDT") → CCXT unified ("BTC/USDT:USDT")
        m_list = c.markets_by_id.get(ccxt_id) or []
        for m in m_list:
            if _is_usdt_linear(m):
                interval_by_sym[m["symbol"]] = _f(entry.get("fundingIntervalHours"))

    funding = await c.fetch_funding_rates()
    tickers = await c.fetch_tickers()

    rows: list[dict] = []
    fanout: list[tuple[str, dict, dict]] = []
    for sym, t in tickers.items():
        market = c.markets.get(sym)
        if not _is_usdt_linear(market):
            continue
        f = funding.get(sym) or {}
        f_info = f.get("info") or {}
        mark = _f(f_info.get("markPrice"))
        index = _f(f_info.get("indexPrice"))
        row = _build_row(
            ts_ms=ts_ms,
            exchange="BINANCE",
            symbol=_canonical_symbol(market),
            base_coin=_base_coin(market),
            base_multiplier=_base_multiplier(market),
            funding_rate=_f(f.get("fundingRate")),
            interval_h=interval_by_sym.get(sym),
            next_ts=_f(f_info.get("nextFundingTime")),
            mark=mark,
            index=index,
            last=_f(t.get("last")),
            oi_usd=None,  # filled in fan-out
            vol_usd=_f(t.get("quoteVolume")),
        )
        rows.append(row)
        fanout.append((sym, market, row))

    sem = asyncio.Semaphore(40)  # public /openInterest weight=1, 2400/min
    failures: Counter[str] = Counter()

    async def _one(sym, market, row):
        async with sem:
            try:
                oi = await c.fetch_open_interest(sym)
            except Exception as e:
                failures[type(e).__name__] += 1
                return
            amt = _f(oi.get("openInterestAmount"))
            if amt is None or row["mark_price"] is None:
                return
            cs = _f(market.get("contractSize")) or 1.0
            row["open_interest_usd"] = amt * cs * row["mark_price"]

    await asyncio.gather(*(_one(s, m, r) for s, m, r in fanout))
    _log_fanout("BINANCE", "OI", len(fanout), failures)
    return rows


# -- BINGX -------------------------------------------------------------------


async def collect_bingx(c, ts_ms):
    """Sources:
    funding_rate     : batch f.unified.fundingRate
    interval_h       : batch f.info.fundingIntervalHours
    next_funding_ts  : batch f.info.nextFundingTime
    mark             : batch f.info.markPrice
    index            : batch f.info.indexPrice
    oi_usd           : per-symbol openInterestValue (already USD direct)
    vol_usd          : t.unified.quoteVolume
    predicted        : not exposed → NULL
    """
    funding = await c.fetch_funding_rates()
    tickers = await c.fetch_tickers()

    rows: list[dict] = []
    fanout: list[tuple[str, dict]] = []
    for sym, t in tickers.items():
        market = c.markets.get(sym)
        if not _is_usdt_linear(market):
            continue
        f = funding.get(sym) or {}
        f_info = f.get("info") or {}
        row = _build_row(
            ts_ms=ts_ms,
            exchange="BINGX",
            symbol=_canonical_symbol(market),
            base_coin=_base_coin(market),
            base_multiplier=_base_multiplier(market),
            funding_rate=_f(f.get("fundingRate")),
            interval_h=_f(f_info.get("fundingIntervalHours")),
            next_ts=_f(f_info.get("nextFundingTime")),
            mark=_f(f_info.get("markPrice")),
            index=_f(f_info.get("indexPrice")),
            last=_f(t.get("last")),
            oi_usd=None,
            vol_usd=_f(t.get("quoteVolume")),
        )
        rows.append(row)
        fanout.append((sym, row))

    sem = asyncio.Semaphore(40)
    failures: Counter[str] = Counter()

    async def _one(sym, row):
        async with sem:
            try:
                oi = await c.fetch_open_interest(sym)
            except Exception as e:
                failures[type(e).__name__] += 1
                return
            row["open_interest_usd"] = _f(oi.get("openInterestValue"))

    await asyncio.gather(*(_one(s, r) for s, r in fanout))
    _log_fanout("BINGX", "OI", len(fanout), failures)
    return rows


# -- BITGET ------------------------------------------------------------------


async def collect_bitget(c, ts_ms):
    """Sources:
    funding_rate     : t.info.fundingRate
    interval_h       : market.info.fundInterval (loaded once at startup)
    next_funding_ts  : derived UTC-aligned from interval
                       (batch funding strips per-symbol nextUpdate; the
                       per-symbol fan-out for 543 symbols is too costly)
    mark             : t.info.markPrice
    index            : t.info.indexPrice
    oi_usd           : t.info.holdingAmount × mark
    vol_usd          : t.info.usdtVolume
    predicted        : not exposed → NULL
    """
    tickers = await c.fetch_tickers()
    rows: list[dict] = []
    for sym, t in tickers.items():
        market = c.markets.get(sym)
        if not _is_usdt_linear(market):
            continue
        m_info = market.get("info") or {}
        t_info = t.get("info") or {}
        mark = _f(t_info.get("markPrice"))
        oi_base = _f(t_info.get("holdingAmount"))
        oi_usd = oi_base * mark if (oi_base is not None and mark is not None) else None
        rows.append(
            _build_row(
                ts_ms=ts_ms,
                exchange="BITGET",
                symbol=_canonical_symbol(market),
                base_coin=_base_coin(market),
                base_multiplier=_base_multiplier(market),
                funding_rate=_f(t_info.get("fundingRate")),
                interval_h=_f(m_info.get("fundInterval")),
                next_ts=None,  # derived in _build_row
                mark=mark,
                index=_f(t_info.get("indexPrice")),
                last=_f(t.get("last")),
                oi_usd=oi_usd,
                vol_usd=_f(t_info.get("usdtVolume")),
            )
        )
    return rows


# -- BITMART -----------------------------------------------------------------


async def collect_bitmart(c, ts_ms):
    """Sources:
    funding_rate     : t.info.expected_funding_rate (the upcoming-cycle
                       refinement; CCXT maps this → unified.fundingRate.
                       Note: t.info.funding_rate is the LAST-settled
                       rate (historical) on bitmart, not what we want.)
    interval_h       : t.info.funding_interval_hours
    next_funding_ts  : t.info.funding_time
    mark             : not exposed → NULL
    index            : t.info.index_price
    oi_usd           : t.info.open_interest_value (USD direct)
    vol_usd          : t.info.turnover_24h
    predicted        : not exposed (bitmart's "expected" is upcoming, not forecast) → NULL
    """
    tickers = await c.fetch_tickers()
    rows: list[dict] = []
    for sym, t in tickers.items():
        market = c.markets.get(sym)
        if not _is_usdt_linear(market):
            continue
        info = t.get("info") or {}
        rows.append(
            _build_row(
                ts_ms=ts_ms,
                exchange="BITMART",
                symbol=_canonical_symbol(market),
                base_coin=_base_coin(market),
                base_multiplier=_base_multiplier(market),
                funding_rate=_f(info.get("expected_funding_rate")),
                interval_h=_f(info.get("funding_interval_hours")),
                next_ts=_f(info.get("funding_time")),
                mark=None,
                index=_f(info.get("index_price")),
                last=_f(t.get("last")),
                oi_usd=_f(info.get("open_interest_value")),
                vol_usd=_f(info.get("turnover_24h")),
            )
        )
    return rows


# -- BYBIT -------------------------------------------------------------------


async def collect_bybit(c, ts_ms):
    """Sources:
    funding_rate     : t.info.fundingRate
    interval_h       : t.info.fundingIntervalHour
    next_funding_ts  : t.info.nextFundingTime
    mark             : t.info.markPrice
    index            : t.info.indexPrice
    oi_usd           : t.info.openInterestValue (USD direct)
    vol_usd          : t.info.turnover24h
    predicted        : not exposed → NULL
    """
    tickers = await c.fetch_tickers()
    rows: list[dict] = []
    for sym, t in tickers.items():
        market = c.markets.get(sym)
        if not _is_usdt_linear(market):
            continue
        info = t.get("info") or {}
        rows.append(
            _build_row(
                ts_ms=ts_ms,
                exchange="BYBIT",
                symbol=_canonical_symbol(market),
                base_coin=_base_coin(market),
                base_multiplier=_base_multiplier(market),
                funding_rate=_f(info.get("fundingRate")),
                interval_h=_f(info.get("fundingIntervalHour")),
                next_ts=_f(info.get("nextFundingTime")),
                mark=_f(info.get("markPrice")),
                index=_f(info.get("indexPrice")),
                last=_f(t.get("last")),
                oi_usd=_f(info.get("openInterestValue")),
                vol_usd=_f(info.get("turnover24h")),
            )
        )
    return rows


# -- COINEX ------------------------------------------------------------------


async def collect_coinex(c, ts_ms):
    """Sources:
    funding_rate     : batch f.unified.fundingRate
    interval_h       : batch f.unified.interval (CCXT parses '8h' → 8)
    next_funding_ts  : batch f.unified.fundingTimestamp  (the upcoming
                       boundary. CCXT's nextFundingTimestamp is the
                       cycle AFTER upcoming — wrong field for our use.)
    mark             : t.info.mark_price
    index            : t.info.index_price
    oi_usd           : t.info.open_interest_volume (base) × mark
    vol_usd          : t.info.value
    predicted        : batch f.info.next_funding_rate (forward forecast)
    """
    funding_task = asyncio.create_task(c.fetch_funding_rates())
    tickers = await c.fetch_tickers()
    funding = await funding_task

    rows: list[dict] = []
    for sym, t in tickers.items():
        market = c.markets.get(sym)
        if not _is_usdt_linear(market):
            continue
        f = funding.get(sym) or {}
        f_info = f.get("info") or {}
        t_info = t.get("info") or {}
        mark = _f(t_info.get("mark_price"))
        oi_base = _f(t_info.get("open_interest_volume"))
        oi_usd = oi_base * mark if (oi_base is not None and mark is not None) else None
        rows.append(
            _build_row(
                ts_ms=ts_ms,
                exchange="COINEX",
                symbol=_canonical_symbol(market),
                base_coin=_base_coin(market),
                base_multiplier=_base_multiplier(market),
                funding_rate=_f(f.get("fundingRate")),
                interval_h=_parse_interval_str(f.get("interval")),
                next_ts=_f(f.get("fundingTimestamp")),
                mark=mark,
                index=_f(t_info.get("index_price")),
                last=_f(t.get("last")),
                oi_usd=oi_usd,
                vol_usd=_f(t_info.get("value")),
                predicted=_f(f_info.get("next_funding_rate")),
            )
        )
    return rows


# -- GATE.IO -----------------------------------------------------------------


async def collect_gate(c, ts_ms):
    """Sources:
    funding_rate     : batch f.unified.fundingRate
    interval_h       : batch f.unified.interval
    next_funding_ts  : batch f.unified.fundingTimestamp
    mark             : t.info.mark_price
    index            : t.info.index_price
    oi_usd           : t.info.total_size × contractSize × mark
    vol_usd          : t.info.volume_24h_quote
    predicted        : NULL (funding_rate_indicative is identical to
                       funding_rate, NOT a forward forecast)
    """
    funding_task = asyncio.create_task(c.fetch_funding_rates())
    tickers = await c.fetch_tickers()
    funding = await funding_task

    rows: list[dict] = []
    for sym, t in tickers.items():
        market = c.markets.get(sym)
        if not _is_usdt_linear(market):
            continue
        f = funding.get(sym) or {}
        t_info = t.get("info") or {}
        mark = _f(t_info.get("mark_price"))
        contracts = _f(t_info.get("total_size"))
        cs = _f(market.get("contractSize")) or 1.0
        oi_usd = (
            contracts * cs * mark
            if (contracts is not None and mark is not None)
            else None
        )
        rows.append(
            _build_row(
                ts_ms=ts_ms,
                exchange="GATE.IO",
                symbol=_canonical_symbol(market),
                base_coin=_base_coin(market),
                base_multiplier=_base_multiplier(market),
                funding_rate=_f(f.get("fundingRate")),
                interval_h=_parse_interval_str(f.get("interval")),
                next_ts=_f(f.get("fundingTimestamp")),
                mark=mark,
                index=_f(t_info.get("index_price")),
                last=_f(t.get("last")),
                oi_usd=oi_usd,
                vol_usd=_f(t_info.get("volume_24h_quote")),
            )
        )
    return rows


# -- HTX ---------------------------------------------------------------------


async def collect_htx(c, ts_ms):
    """Sources:
    funding_rate     : batch f.unified.fundingRate
    interval_h       : market.info.settlement_period (loaded once)
    next_funding_ts  : batch f.info.next_funding_time
    mark             : not exposed → NULL
    index            : not exposed → NULL
    oi_usd           : batch fetchOpenInterests → openInterestValue (USD direct)
    vol_usd          : t.info.trade_turnover (override; unified.quoteVolume is wrong)
    predicted        : batch f.info.estimated_rate (forward forecast,
                       often null when venue hasn't computed one yet)
    """
    funding_task = asyncio.create_task(c.fetch_funding_rates())
    oi_task = asyncio.create_task(c.fetch_open_interests())
    tickers = await c.fetch_tickers()
    funding = await funding_task
    oi_batch = await oi_task

    rows: list[dict] = []
    for sym, t in tickers.items():
        market = c.markets.get(sym)
        if not _is_usdt_linear(market):
            continue
        m_info = market.get("info") or {}
        f = funding.get(sym) or {}
        f_info = f.get("info") or {}
        t_info = t.get("info") or {}
        oi_obj = oi_batch.get(sym) or {}
        rows.append(
            _build_row(
                ts_ms=ts_ms,
                exchange="HTX",
                symbol=_canonical_symbol(market),
                base_coin=_base_coin(market),
                base_multiplier=_base_multiplier(market),
                funding_rate=_f(f.get("fundingRate")),
                interval_h=_f(m_info.get("settlement_period")),
                next_ts=_f(f_info.get("next_funding_time")),
                mark=None,
                index=None,
                last=_f(t.get("last")),
                oi_usd=_f(oi_obj.get("openInterestValue")),
                vol_usd=_f(t_info.get("trade_turnover")),
                predicted=_f(f_info.get("estimated_rate")),
            )
        )
    return rows


# -- KUCOIN ------------------------------------------------------------------


async def collect_kucoin(c, ts_ms):
    """KUCOIN's batch fetch_tickers carries everything we need; the
    single-symbol fetch_ticker is a tick feed and missed data.

    Sources:
      funding_rate     : t.info.fundingFeeRate
      interval_h       : t.info.fundingRateGranularity (milliseconds → ÷ 3.6e6)
                         Verified: BTC = 28800000 ms = 8 h; LAB = 14400000 ms = 4 h.
      next_funding_ts  : t.info.nextFundingRateDateTime  (this IS an
                         absolute ms timestamp despite the "DateTime" name;
                         t.info.nextFundingRateTime is a countdown duration,
                         do NOT use that one.)
      mark             : t.info.markPrice
      index            : t.info.indexPrice
      oi_usd           : t.info.openInterest × contractSize × mark
      vol_usd          : t.info.turnoverOf24h
      predicted        : t.info.predictedFundingFeeRate (forward forecast;
                         frequently None at quiet moments)
    """
    tickers = await c.fetch_tickers()
    rows: list[dict] = []
    for sym, t in tickers.items():
        market = c.markets.get(sym)
        if not _is_usdt_linear(market):
            continue
        info = t.get("info") or {}
        gran_ms = _f(info.get("fundingRateGranularity"))
        interval_h = gran_ms / 3_600_000.0 if gran_ms else None
        mark = _f(info.get("markPrice"))
        oi_base = _f(info.get("openInterest"))
        cs = _f(market.get("contractSize")) or 1.0
        oi_usd = (
            oi_base * cs * mark if (oi_base is not None and mark is not None) else None
        )
        rows.append(
            _build_row(
                ts_ms=ts_ms,
                exchange="KUCOIN",
                symbol=_canonical_symbol(market),
                base_coin=_base_coin(market),
                base_multiplier=_base_multiplier(market),
                funding_rate=_f(info.get("fundingFeeRate")),
                interval_h=interval_h,
                next_ts=_f(info.get("nextFundingRateDateTime")),
                mark=mark,
                index=_f(info.get("indexPrice")),
                last=_f(t.get("last")),
                oi_usd=oi_usd,
                vol_usd=_f(info.get("turnoverOf24h")),
                predicted=_f(info.get("predictedFundingFeeRate")),
            )
        )
    return rows


# -- MEXC --------------------------------------------------------------------

_MEXC_NATIVE_FUNDING = "https://contract.mexc.com/api/v1/contract/funding_rate"


async def collect_mexc(c, ts_ms):
    """MEXC has no batch fetchFundingRates in CCXT, but the venue's own
    /api/v1/contract/funding_rate endpoint returns funding+interval+
    nextSettleTime for every symbol in one call. We use that.

    Sources:
      funding_rate     : native HTTP fundingRate
      interval_h       : native HTTP collectCycle
      next_funding_ts  : native HTTP nextSettleTime
      mark             : t.info.fairPrice
      index            : t.info.indexPrice
      oi_usd           : t.info.holdVol × contractSize × mark
      vol_usd          : t.info.amount24
      predicted        : not exposed → NULL
    """
    tickers_task = asyncio.create_task(c.fetch_tickers())

    # Native HTTP — uses the same threaded-DNS session pattern.
    connector = aiohttp.TCPConnector(
        resolver=aiohttp.ThreadedResolver(), ttl_dns_cache=300
    )
    async with aiohttp.ClientSession(connector=connector, trust_env=True) as s:
        async with s.get(
            _MEXC_NATIVE_FUNDING, timeout=aiohttp.ClientTimeout(total=15)
        ) as r:
            body = await r.json()
    by_id = {e.get("symbol"): e for e in (body.get("data") or [])}

    tickers = await tickers_task

    rows: list[dict] = []
    for sym, t in tickers.items():
        market = c.markets.get(sym)
        if not _is_usdt_linear(market):
            continue
        # MEXC native ids look like "BTC_USDT"; CCXT exposes id on the market.
        venue_id = market.get("id")
        native = by_id.get(venue_id) or {}
        info = t.get("info") or {}
        mark = _f(info.get("fairPrice"))
        contracts = _f(info.get("holdVol"))
        cs = _f(market.get("contractSize")) or 1.0
        oi_usd = (
            contracts * cs * mark
            if (contracts is not None and mark is not None)
            else None
        )
        rows.append(
            _build_row(
                ts_ms=ts_ms,
                exchange="MEXC",
                symbol=_canonical_symbol(market),
                base_coin=_base_coin(market),
                base_multiplier=_base_multiplier(market),
                funding_rate=_f(native.get("fundingRate")),
                interval_h=_f(native.get("collectCycle")),
                next_ts=_f(native.get("nextSettleTime")),
                mark=mark,
                index=_f(info.get("indexPrice")),
                last=_f(t.get("last")),
                oi_usd=oi_usd,
                vol_usd=_f(info.get("amount24")),
            )
        )
    return rows


# -- OKX ---------------------------------------------------------------------


async def collect_okx(c, ts_ms):
    """OKX exposes neither mark nor index via fetch_ticker, and
    quoteVolume is not populated. We compute USD volume from
    baseVolume × contractSize × last (the only available price).
    Mark and index are stored as NULL.

    Sources:
      funding_rate     : batch f.unified.fundingRate
      interval_h       : batch f.unified.interval
      next_funding_ts  : batch f.unified.fundingTimestamp  (the upcoming
                         boundary. CCXT's nextFundingTimestamp is the
                         cycle AFTER upcoming — wrong field for our use.)
      mark             : NULL (not exposed)
      index            : NULL (not exposed)
      oi_usd           : batch fetchOpenInterests → openInterestValue (USD direct)
      vol_usd          : t.unified.baseVolume × contractSize × t.unified.last
      predicted        : batch f.info.nextFundingRate (skip if empty string)
    """
    funding_task = asyncio.create_task(c.fetch_funding_rates())
    oi_task = asyncio.create_task(c.fetch_open_interests())
    tickers = await c.fetch_tickers()
    funding = await funding_task
    oi_batch = await oi_task

    rows: list[dict] = []
    for sym, t in tickers.items():
        market = c.markets.get(sym)
        if not _is_usdt_linear(market):
            continue
        f = funding.get(sym) or {}
        f_info = f.get("info") or {}
        oi_obj = oi_batch.get(sym) or {}
        last = _f(t.get("last"))
        bv = _f(t.get("baseVolume"))
        cs = _f(market.get("contractSize")) or 1.0
        vol_usd = (bv * cs * last) if (bv is not None and last is not None) else None
        rows.append(
            _build_row(
                ts_ms=ts_ms,
                exchange="OKX",
                symbol=_canonical_symbol(market),
                base_coin=_base_coin(market),
                base_multiplier=_base_multiplier(market),
                funding_rate=_f(f.get("fundingRate")),
                interval_h=_parse_interval_str(f.get("interval")),
                next_ts=_f(f.get("fundingTimestamp")),
                mark=None,
                index=None,
                last=last,
                oi_usd=_f(oi_obj.get("openInterestValue")),
                vol_usd=vol_usd,
                predicted=_f(f_info.get("nextFundingRate")),  # _f rejects ""
            )
        )
    return rows


# -- PHEMEX ------------------------------------------------------------------


async def collect_phemex(c, ts_ms):
    """Sources:
    funding_rate     : t.info.fundingRateRr
    interval_h       : market.info.fundingInterval (seconds → ÷ 3600)
    next_funding_ts  : derived UTC-aligned (phemex publishes neither
                       next-funding-time in ticker nor a per-symbol
                       funding endpoint with a usable timestamp)
    mark             : t.info.markPriceRp
    index            : t.info.indexPriceRp
    oi_usd           : t.info.openInterestRv × mark
    vol_usd          : t.info.turnoverRv
    predicted        : t.info.predFundingRateRr (forward forecast)
    """
    tickers = await c.fetch_tickers()
    rows: list[dict] = []
    for sym, t in tickers.items():
        market = c.markets.get(sym)
        if not _is_usdt_linear(market):
            continue
        m_info = market.get("info") or {}
        info = t.get("info") or {}
        sec = _f(m_info.get("fundingInterval"))
        interval_h = (sec / 3600.0) if sec else None
        mark = _f(info.get("markPriceRp"))
        oi_base = _f(info.get("openInterestRv"))
        oi_usd = oi_base * mark if (oi_base is not None and mark is not None) else None
        rows.append(
            _build_row(
                ts_ms=ts_ms,
                exchange="PHEMEX",
                symbol=_canonical_symbol(market),
                base_coin=_base_coin(market),
                base_multiplier=_base_multiplier(market),
                funding_rate=_f(info.get("fundingRateRr")),
                interval_h=interval_h,
                next_ts=None,  # derived in _build_row
                mark=mark,
                index=_f(info.get("indexPriceRp")),
                last=_f(t.get("last")),
                oi_usd=oi_usd,
                vol_usd=_f(info.get("turnoverRv")),
                predicted=_f(info.get("predFundingRateRr")),
            )
        )
    return rows


# -- XT.COM ------------------------------------------------------------------


async def collect_xt(c, ts_ms):
    """XT.COM has no batch fetchFundingRates and no OI exposure. We
    fan out per-symbol fetch_funding_rate to get the funding rate,
    interval, and next-settlement timestamp.

    Sources (per-symbol via fetch_funding_rate):
      funding_rate     : f.unified.fundingRate
      interval_h       : f.info.collectionInternal (typo for 'Interval')
      next_funding_ts  : f.info.nextCollectionTime
    Plus from batch tickers:
      mark             : t.info.m
      index            : t.info.i
      oi_usd           : NULL (not exposed anywhere)
      vol_usd          : t.unified.quoteVolume
      predicted        : not exposed → NULL
    """
    tickers = await c.fetch_tickers()
    targets = [
        (s, m, t)
        for s, t in tickers.items()
        if (m := c.markets.get(s)) and _is_usdt_linear(m)
    ]

    sem = asyncio.Semaphore(40)
    failures: Counter[str] = Counter()

    async def _one(sym, market, t):
        async with sem:
            try:
                f = await c.fetch_funding_rate(sym)
            except Exception as e:
                failures[type(e).__name__] += 1
                return None
        f_info = f.get("info") or {}
        info = t.get("info") or {}
        return _build_row(
            ts_ms=ts_ms,
            exchange="XT.COM",
            symbol=_canonical_symbol(market),
            base_coin=_base_coin(market),
            base_multiplier=_base_multiplier(market),
            funding_rate=_f(f.get("fundingRate")),
            interval_h=_f(f_info.get("collectionInternal")),
            next_ts=_f(f_info.get("nextCollectionTime")),
            mark=_f(info.get("m")),
            index=_f(info.get("i")),
            last=_f(t.get("last")),
            oi_usd=None,
            vol_usd=_f(t.get("quoteVolume")),
        )

    results = await asyncio.gather(*(_one(s, m, t) for s, m, t in targets))
    _log_fanout("XT.COM", "funding", len(targets), failures)
    return [r for r in results if r is not None]


# ---------------------------------------------------------------------------
# Misc helpers
# ---------------------------------------------------------------------------


def _parse_interval_str(s) -> float | None:
    """Parse CCXT's unified `interval` field, which arrives as '8h'/'4h'/
    '1h'. Returns hours as float, or None."""
    if not isinstance(s, str) or not s.endswith("h"):
        return None
    try:
        return float(s.rstrip("h"))
    except ValueError:
        return None


COLLECTORS = {
    "BINANCE": collect_binance,
    "BINGX": collect_bingx,
    "BITGET": collect_bitget,
    "BITMART": collect_bitmart,
    "BYBIT": collect_bybit,
    "COINEX": collect_coinex,
    "GATE.IO": collect_gate,
    "HTX": collect_htx,
    "KUCOIN": collect_kucoin,
    "MEXC": collect_mexc,
    "OKX": collect_okx,
    "PHEMEX": collect_phemex,
    "XT.COM": collect_xt,
}


# ---------------------------------------------------------------------------
# venue loop and main
# ---------------------------------------------------------------------------


async def venue_loop(canonical: str, stop: asyncio.Event, max_cycles: int = -1):
    collector = COLLECTORS[canonical]
    cycles = 0
    async with open_client(canonical) as c:
        try:
            await c.load_markets()
        except Exception as e:
            log.error(
                "[%s] load_markets failed: %s: %s",
                canonical,
                type(e).__name__,
                str(e)[:200],
            )
            return
        log.info("[%s] markets loaded (%d total)", canonical, len(c.markets))
        last_reload = time.monotonic()
        while not stop.is_set():
            cycle_start = time.monotonic()

            # Periodic market-metadata refresh — picks up newly-listed (or
            # removed) symbols on the venue without restarting the loop.
            # Per-venue extractors read intervals/contract sizes from
            # market.info, so a fresh load_markets is the only thing
            # required for new pairs to flow through correctly. On
            # reload failure we keep the previous cache and try again
            # one full interval later (no aggressive retry — protects
            # against cascading load on a degraded venue).
            if cycle_start - last_reload >= MARKETS_RELOAD_INTERVAL_S:
                prev_count = len(c.markets)
                try:
                    await c.load_markets(reload=True)
                    new_count = len(c.markets)
                    log.info(
                        "[%s] markets reloaded: %d total (%+d)",
                        canonical,
                        new_count,
                        new_count - prev_count,
                    )
                except Exception as e:
                    log.warning(
                        "[%s] markets reload failed: %s: %s",
                        canonical,
                        type(e).__name__,
                        str(e)[:200],
                    )
                last_reload = cycle_start

            ts_ms = int(time.time() * 1000)
            try:
                rows = await collector(c, ts_ms)
                _write_partition(canonical, ts_ms, rows)
                log.info(
                    "[%s] cycle ok: %d rows in %.1fs",
                    canonical,
                    len(rows),
                    time.monotonic() - cycle_start,
                )
            except Exception as e:
                log.error(
                    "[%s] cycle failed: %s: %s",
                    canonical,
                    type(e).__name__,
                    str(e)[:200],
                )
            cycles += 1
            if max_cycles > 0 and cycles >= max_cycles:
                return
            elapsed = time.monotonic() - cycle_start
            with suppress(asyncio.TimeoutError):
                await asyncio.wait_for(
                    stop.wait(), timeout=max(0.0, POLL_INTERVAL_S - elapsed)
                )


async def amain(args):
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%H:%M:%S",
    )
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop.set)
        except (NotImplementedError, RuntimeError):
            pass

    venues = list(COLLECTORS) if not args.venues else args.venues
    log.info(
        "starting %d venues: %s (cycles=%s)",
        len(venues),
        ", ".join(venues),
        args.cycles if args.cycles > 0 else "inf",
    )
    log.info("data dir: %s", FUNDING_DIR.resolve())

    tasks = [asyncio.create_task(venue_loop(v, stop, args.cycles)) for v in venues]
    await asyncio.gather(*tasks)
    log.info("collector stopped")


def main():
    p = argparse.ArgumentParser(description="Funding/OI/volume collector")
    p.add_argument("--once", action="store_true", help="Shortcut for --cycles 1.")
    p.add_argument(
        "--cycles",
        type=int,
        default=-1,
        help="Run N cycles per venue and exit (-1 = run forever).",
    )
    p.add_argument(
        "--venues",
        nargs="*",
        default=None,
        help="Subset of venues to run (default: all configured).",
    )
    args = p.parse_args()
    if args.once:
        args.cycles = 1
    try:
        asyncio.run(amain(args))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
