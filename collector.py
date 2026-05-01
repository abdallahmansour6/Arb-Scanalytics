"""
Async collector. One task per venue.

Each task pulls funding/OI/mark/volume for every USDT-linear perp on its
venue, normalizes to the canonical row shape, and appends one Parquet
file to the active partition (data/funding/year=YYYY/month=MM/day=DD/).

Per-venue strategies are bespoke because each venue scatters funding/OI
across different surfaces (ticker.info, batch endpoints, per-symbol
fan-out). The variation map was established empirically via probes.py
and is encoded inline below.

Failure of any single venue is isolated and logged; sibling venues
continue. Run with --once for a single cycle (test-drive); without flags,
loops until SIGTERM/SIGINT.
"""

import argparse
import asyncio
import logging
import signal
import time
from contextlib import suppress
from datetime import datetime, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from config import VENUES, FUNDING_DIR, POLL_INTERVAL_S, open_client


# ---------------------------------------------------------------------------
# Canonical schema (locked at collector boundary)
# ---------------------------------------------------------------------------
#
# Venues expose up to THREE distinct funding-rate concepts; the schema below
# stores two and ignores the third:
#
#   (A) LAST-SETTLED    — already-paid rate from the previous boundary.
#                         Historical. Only BITMART exposes a dedicated field
#                         for this (info.funding_rate). Not stored.
#   (B) UPCOMING        — the rate that will settle at the next boundary.
#                         Refines continuously as the TWAP-of-premium-index
#                         accumulates over the cycle. This is what a position
#                         opened *now* will pay/receive at next_funding_ts.
#                         Stored as: funding_rate. CCXT's unified fundingRate
#                         always points here.
#   (C) FORWARD FORECAST — exchange's prediction for the cycle AFTER upcoming.
#                          Only 4 venues expose this: COINEX (info.next_funding_rate),
#                          HTX (info.estimated_rate), OKX (info.nextFundingRate),
#                          PHEMEX (info.predFundingRateRr). Stored as:
#                          predicted_rate. Null for the other 9 venues.
#
# Common labels in the wild ("Expected Funding Rate", "Predicted Rate") are
# venue-specific and inconsistent — always reduce to (A)/(B)/(C) when reading
# venue docs.

SCHEMA = pa.schema([
    ("ts_utc",             pa.int64()),    # ms since epoch (UTC)
    ("exchange",           pa.string()),
    ("symbol_canonical",   pa.string()),   # e.g. "BTC/USDT:USDT"
    ("funding_rate",       pa.float64()),  # (B) upcoming-boundary rate, raw per-epoch
    ("funding_interval_h", pa.float32()),  # usually 1/4/8; some venues use other
    ("predicted_rate",     pa.float64()),  # (C) forecast for cycle AFTER upcoming
    ("next_funding_ts",    pa.int64()),    # ms; settlement of the upcoming epoch
    ("mark_price",         pa.float64()),
    ("index_price",        pa.float64()),
    ("open_interest_usd",  pa.float64()),
    ("volume_24h_usd",     pa.float64()),
    ("apy_norm",           pa.float64()),  # rate * 8760 / interval_h
])


log = logging.getLogger("scanner.collector")


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def _is_usdt_linear(market: dict | None) -> bool:
    """USDT-quoted, USDT-settled, linear, swap, not deactivated.

    Field-notes from a prior pass said coinex's natural BTC perp is
    BTC/USD:USDT (quote=USD, settle=USDT) and that requiring quote==USDT
    would drop the venue entirely. Probes against current CCXT 4.5.49
    show coinex offers BTC/USDT:USDT directly — the /USD:USDT line is
    either retired or moved to a different listing class. Requiring
    quote==USDT AND settle==USDT prevents per-venue duplicates from
    USDC-quoted or USD-quoted variants of the same base. Treat
    active=None as pass (coinex's own quirk)."""
    if not market or not market.get("swap") or not market.get("linear"):
        return False
    if market.get("active") is False:
        return False
    return market.get("quote") == "USDT" and market.get("settle") == "USDT"


def _canonical_symbol(market: dict) -> str:
    """CCXT unified symbol — already the canonical truth on the wire."""
    return market["symbol"]


def _f(v):
    """Defensive float coerce. Empty string and None both become None."""
    if v is None or v == "":
        return None
    try:
        return float(v)
    except (ValueError, TypeError):
        return None


def _interval_h(unified_funding: dict | None, ticker_info: dict | None,
                default: float = 8.0) -> float:
    """Resolve epoch length in hours.

    Priority: unified funding 'interval' string ("8h"), then unified
    nextFundingTimestamp - fundingTimestamp diff, then any common ticker-
    info field that carries the interval, then default (8h)."""
    if unified_funding:
        iv = unified_funding.get("interval")
        if isinstance(iv, str) and iv.endswith("h"):
            try:
                return float(iv.rstrip("h"))
            except ValueError:
                pass
        fts = unified_funding.get("fundingTimestamp")
        nfts = unified_funding.get("nextFundingTimestamp")
        if fts and nfts and nfts > fts:
            return (nfts - fts) / 3_600_000
    if ticker_info:
        for k in ("funding_interval_hours", "fundingIntervalHour",
                  "collectCycle", "collectionInternal"):
            v = _f(ticker_info.get(k))
            if v:
                return v
    return default


def _apy_norm(rate, interval_h):
    if rate is None or not interval_h:
        return None
    return rate * 8760.0 / interval_h


def _next_ts_from_interval(ts_ms: int, interval_h: float | None) -> int | None:
    """UTC-aligned next funding boundary. Heuristic for venues that don't
    expose next_funding_ts directly (currently MEXC).

    Verified empirically against 12 of our 13 venues: all 8h cycles align
    to the same UTC-anchored grid (their nextFundingTimestamp values match
    across venues for identical symbols). For 1h/4h cycles the same UTC
    alignment held in samples observed. If a venue ever offsets its
    boundaries, this heuristic will be wrong by the offset."""
    if not interval_h:
        return None
    interval_ms = int(interval_h * 3600 * 1000)
    return ((ts_ms // interval_ms) + 1) * interval_ms


def _row(*, ts_ms, exchange, symbol, funding_rate, interval_h,
         predicted=None, next_ts=None, mark=None, index=None,
         oi_usd=None, vol_usd=None) -> dict:
    if not next_ts and interval_h:
        next_ts = _next_ts_from_interval(ts_ms, interval_h)
    return {
        "ts_utc": ts_ms,
        "exchange": exchange,
        "symbol_canonical": symbol,
        "funding_rate": funding_rate,
        "funding_interval_h": float(interval_h) if interval_h else None,
        "predicted_rate": predicted,
        "next_funding_ts": int(next_ts) if next_ts else None,
        "mark_price": mark,
        "index_price": index,
        "open_interest_usd": oi_usd,
        "volume_24h_usd": vol_usd,
        "apy_norm": _apy_norm(funding_rate, interval_h),
    }


def _write_partition(exchange: str, ts_ms: int, rows: list[dict]):
    """Append one Parquet file per (exchange, cycle) into the day partition."""
    if not rows:
        return
    now = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)
    partition = (FUNDING_DIR
                 / f"year={now.year:04d}"
                 / f"month={now.month:02d}"
                 / f"day={now.day:02d}")
    partition.mkdir(parents=True, exist_ok=True)
    file = partition / f"{exchange}_{ts_ms}.parquet"
    table = pa.Table.from_pylist(rows, schema=SCHEMA)
    pq.write_table(table, file, compression="snappy")


# ---------------------------------------------------------------------------
# venue collectors
#
# Each takes (client, ts_ms) and returns list[canonical_row].
# Strategies and field map come from probes.py findings + FIELD_NOTES.md.
# ---------------------------------------------------------------------------


# -- Helpers shared across the three collection paths -----------------------

def _extract_oi_usd(info: dict, oi_field: str | None, oi_unit: str,
                    mark: float | None, market: dict) -> float | None:
    if not oi_field:
        return None
    v = _f(info.get(oi_field))
    if v is None:
        return None
    if oi_unit == "usd":
        return v
    if mark is None:
        return None
    if oi_unit == "base":
        return v * mark
    if oi_unit == "contracts":
        cs = _f(market.get("contractSize")) or 1.0
        return v * cs * mark
    return None


def _extract_vol_usd(t: dict, info: dict, vol_field: str | None,
                     mark: float | None, market: dict) -> float | None:
    """USD volume. Priority: ticker.info[vol_field] -> unified.quoteVolume ->
    baseVolume * contractSize * mark (handles OKX, where unified.quoteVolume
    is None and baseVolume is in raw contracts)."""
    if vol_field:
        v = _f(info.get(vol_field))
        if v is not None:
            return v
    v = _f(t.get("quoteVolume"))
    if v is not None:
        return v
    bv = _f(t.get("baseVolume"))
    if bv is not None and mark:
        cs = _f(market.get("contractSize")) or 1.0
        return bv * cs * mark
    return None


# -- Group A: everything we need is in ticker.info (one fetch_tickers call) --

async def _collect_via_ticker_info(c, exchange: str, ts_ms: int, *,
                                    funding_field: str,
                                    predicted_field: str | None = None,
                                    mark_field: str | None = None,
                                    index_field: str | None = None,
                                    oi_field: str | None = None,
                                    oi_unit: str = "base",
                                    vol_field: str | None = None,
                                    next_ts_field: str | None = None,
                                    interval_default: float = 8.0) -> list[dict]:
    tickers = await c.fetch_tickers()
    rows: list[dict] = []
    for sym, t in tickers.items():
        market = c.markets.get(sym)
        if not _is_usdt_linear(market):
            continue
        info = t.get("info") or {}
        mark = _f(info.get(mark_field)) if mark_field else None
        if mark is None:
            mark = _f(t.get("last"))
        index = _f(info.get(index_field)) if index_field else None
        if index is None:
            index = _f(t.get("indexPrice")) or mark

        rows.append(_row(
            ts_ms=ts_ms, exchange=exchange, symbol=_canonical_symbol(market),
            funding_rate=_f(info.get(funding_field)),
            interval_h=_interval_h(None, info, default=interval_default),
            predicted=_f(info.get(predicted_field)) if predicted_field else None,
            next_ts=_f(info.get(next_ts_field)) if next_ts_field else None,
            mark=mark, index=index,
            oi_usd=_extract_oi_usd(info, oi_field, oi_unit, mark, market),
            vol_usd=_extract_vol_usd(t, info, vol_field, mark, market),
        ))
    return rows


async def collect_bitmart(c, ts_ms):
    # Bitmart's ticker.info exposes BOTH last-settled (funding_rate) and the
    # upcoming-boundary refinement (expected_funding_rate). CCXT maps
    # expected_funding_rate -> unified.fundingRate, confirming "expected"
    # is the upcoming rate. We store the upcoming as funding_rate and drop
    # the historical last-settled. Bitmart does not expose a forward
    # forecast for the cycle AFTER upcoming.
    return await _collect_via_ticker_info(
        c, "BITMART", ts_ms,
        funding_field="expected_funding_rate", predicted_field=None,
        index_field="index_price",
        oi_field="open_interest_value", oi_unit="usd",
        vol_field="turnover_24h", next_ts_field="funding_time",
    )


async def collect_bybit(c, ts_ms):
    return await _collect_via_ticker_info(
        c, "BYBIT", ts_ms,
        funding_field="fundingRate",
        mark_field="markPrice", index_field="indexPrice",
        oi_field="openInterestValue", oi_unit="usd",
        vol_field="turnover24h", next_ts_field="nextFundingTime",
    )


async def collect_mexc(c, ts_ms):
    return await _collect_via_ticker_info(
        c, "MEXC", ts_ms,
        funding_field="fundingRate", predicted_field=None,
        mark_field="fairPrice", index_field="indexPrice",
        oi_field="holdVol", oi_unit="contracts",
        vol_field="amount24",
    )


async def collect_phemex(c, ts_ms):
    # Phemex fields use Rr/Rp/Rv suffixes — field-notes warned of scaled Ep/Ev
    # ints elsewhere, but the Rr (real rate) and Rp (real price) fields used
    # here come through descaled (verified empirically against BTC).
    return await _collect_via_ticker_info(
        c, "PHEMEX", ts_ms,
        funding_field="fundingRateRr", predicted_field="predFundingRateRr",
        mark_field="markPriceRp", index_field="indexPriceRp",
        oi_field="openInterestRv", oi_unit="base",
        vol_field="turnoverRv",
    )


# -- Group B: ticker for vol/mark + batch fetch_funding_rates for funding --

async def _collect_with_batch_funding(c, exchange: str, ts_ms: int, *,
                                       predicted_info_key: str | None = None,
                                       oi_strategy: str = "skip",
                                       # ticker.info-based extractions, applied
                                       # in addition to / overriding the unified path:
                                       mark_info_field: str | None = None,
                                       index_info_field: str | None = None,
                                       oi_info_field: str | None = None,
                                       oi_info_unit: str = "base",
                                       vol_info_field: str | None = None,
                                       next_ts_info_field: str | None = None,
                                       interval_default: float = 8.0
                                       ) -> list[dict]:
    """Fetch tickers + batch funding (+ optional batch OI) in parallel,
    join, and produce canonical rows.

    oi_strategy:
      - "batch":              fetch_open_interests() in parallel; merge by symbol
      - "per_symbol_fanout":  fetch_open_interest(sym) per row after the join
      - "skip":               no OI (default)

    If oi_info_field is provided, it takes precedence over oi_strategy
    (extract OI from ticker.info — used by coinex)."""
    tickers_task = asyncio.create_task(c.fetch_tickers())
    funding_task = asyncio.create_task(c.fetch_funding_rates())
    oi_task = (asyncio.create_task(c.fetch_open_interests())
               if oi_strategy == "batch" else None)
    tickers = await tickers_task
    funding = await funding_task
    oi_batch = await oi_task if oi_task else {}

    rows: list[dict] = []
    fanout_targets: list[tuple[str, dict, dict]] = []
    for sym, t in tickers.items():
        market = c.markets.get(sym)
        if not _is_usdt_linear(market):
            continue
        f = funding.get(sym) or {}
        if not f.get("fundingRate") and f.get("fundingRate") != 0.0:
            continue  # skip pairs without a funding rate this cycle
        info_t = t.get("info") or {}
        info_f = f.get("info") or {}

        mark = _f(info_t.get(mark_info_field)) if mark_info_field else None
        if mark is None:
            mark = _f(t.get("markPrice")) or _f(t.get("last"))
        index = _f(info_t.get(index_info_field)) if index_info_field else None
        if index is None:
            index = _f(t.get("indexPrice")) or mark

        oi_usd = None
        if oi_info_field:
            oi_usd = _extract_oi_usd(info_t, oi_info_field, oi_info_unit, mark, market)
        elif oi_strategy == "batch":
            oi_obj = oi_batch.get(sym) or {}
            v = _f(oi_obj.get("openInterestValue"))
            if v is None:
                v = _f(oi_obj.get("openInterestAmount"))
                if v is not None and mark:
                    cs = _f(market.get("contractSize")) or 1.0
                    v = v * cs * mark
            oi_usd = v
        # per_symbol_fanout: filled after the loop

        next_ts = f.get("nextFundingTimestamp") or f.get("fundingTimestamp")
        if next_ts_info_field:
            ti = _f(info_t.get(next_ts_info_field))
            if ti is not None:
                next_ts = ti

        rows.append(_row(
            ts_ms=ts_ms, exchange=exchange,
            symbol=_canonical_symbol(market),
            funding_rate=_f(f.get("fundingRate")),
            interval_h=_interval_h(f, info_t, default=interval_default),
            predicted=_f(info_f.get(predicted_info_key)) if predicted_info_key else None,
            next_ts=next_ts, mark=mark, index=index,
            oi_usd=oi_usd,
            vol_usd=_extract_vol_usd(t, info_t, vol_info_field, mark, market),
        ))
        if oi_strategy == "per_symbol_fanout" and not oi_info_field:
            fanout_targets.append((sym, market, rows[-1]))

    if fanout_targets:
        # Tight sem-bounded fan-out to respect rate limits while filling fast.
        sem = asyncio.Semaphore(20)

        async def _one(sym, market, row):
            async with sem:
                try:
                    oi = await c.fetch_open_interest(sym)
                except Exception:
                    return
                v = _f(oi.get("openInterestValue"))
                if v is None:
                    v = _f(oi.get("openInterestAmount"))
                    if v is not None and row["mark_price"]:
                        cs = _f(market.get("contractSize")) or 1.0
                        v = v * cs * row["mark_price"]
                row["open_interest_usd"] = v

        await asyncio.gather(*(_one(s, m, r) for s, m, r in fanout_targets))

    return rows


async def collect_binance(c, ts_ms):
    # OI: per-symbol fan-out (batch unsupported). ~559 calls/cycle.
    return await _collect_with_batch_funding(
        c, "BINANCE", ts_ms, oi_strategy="per_symbol_fanout",
    )


async def collect_bingx(c, ts_ms):
    # OI: per-symbol fan-out (batch unsupported). ~595 calls/cycle.
    return await _collect_with_batch_funding(
        c, "BINGX", ts_ms, oi_strategy="per_symbol_fanout",
    )


async def collect_bitget(c, ts_ms):
    # Bitget has funding in ticker.info AND batch fetchFundingRates. We
    # use batch funding so we get unified.fundingTimestamp (next epoch ts);
    # ticker.info still provides OI/mark/vol. No predicted in either path.
    return await _collect_with_batch_funding(
        c, "BITGET", ts_ms,
        mark_info_field="markPrice", index_info_field="indexPrice",
        oi_info_field="holdingAmount", oi_info_unit="base",
        vol_info_field="usdtVolume",
    )


async def collect_coinex(c, ts_ms):
    # CCXT advertises fetch_funding_rates; OI/vol/mark sit in ticker.info.
    return await _collect_with_batch_funding(
        c, "COINEX", ts_ms,
        predicted_info_key="next_funding_rate",
        mark_info_field="mark_price", index_info_field="index_price",
        oi_info_field="open_interest_volume", oi_info_unit="base",
        vol_info_field="value",
    )


async def collect_gate(c, ts_ms):
    # Gate.io has funding in ticker.info AND batch fetchFundingRates. We
    # use batch funding for unified.fundingTimestamp (next epoch ts);
    # ticker.info still provides OI/mark/vol.
    return await _collect_with_batch_funding(
        c, "GATE.IO", ts_ms,
        mark_info_field="mark_price", index_info_field="index_price",
        oi_info_field="total_size", oi_info_unit="contracts",
        vol_info_field="volume_24h_quote",
    )


async def collect_htx(c, ts_ms):
    # HTX unified.quoteVolume is wrong (=baseVolume*1000). ticker.info
    # 'trade_turnover' is the real USD turnover.
    return await _collect_with_batch_funding(
        c, "HTX", ts_ms,
        predicted_info_key="estimated_rate",
        oi_strategy="batch",
        vol_info_field="trade_turnover",
    )


async def collect_okx(c, ts_ms):
    # OKX has no quoteVolume in unified ticker; volume falls back to
    # baseVolume * contractSize * mark inside _extract_vol_usd.
    return await _collect_with_batch_funding(
        c, "OKX", ts_ms,
        predicted_info_key="nextFundingRate",
        oi_strategy="batch",
    )


# -- Group C: per-symbol funding fan-out --

async def _collect_with_funding_fanout(c, exchange: str, ts_ms: int, *,
                                        oi_strategy: str = "skip",
                                        mark_info_field: str | None = None,
                                        index_info_field: str | None = None,
                                        vol_info_field: str | None = None,
                                        ) -> list[dict]:
    """fetch_tickers + per-symbol fetch_funding_rate fan-out.
    Optional batch OI; per-venue ticker.info field overrides."""
    tickers_task = asyncio.create_task(c.fetch_tickers())
    oi_task = (asyncio.create_task(c.fetch_open_interests())
               if oi_strategy == "batch" else None)
    tickers = await tickers_task
    oi_batch = await oi_task if oi_task else {}

    targets = [(s, m, t) for s, t in tickers.items()
               if (m := c.markets.get(s)) and _is_usdt_linear(m)]

    sem = asyncio.Semaphore(20)

    async def _one(sym, market, t):
        async with sem:
            try:
                f = await c.fetch_funding_rate(sym)
            except Exception:
                return None
        info_t = t.get("info") or {}
        mark = _f(info_t.get(mark_info_field)) if mark_info_field else None
        if mark is None:
            mark = _f(t.get("markPrice")) or _f(t.get("last"))
        index = _f(info_t.get(index_info_field)) if index_info_field else None
        if index is None:
            index = _f(t.get("indexPrice")) or mark

        oi_usd = None
        if oi_strategy == "batch":
            oi_obj = oi_batch.get(sym) or {}
            v = _f(oi_obj.get("openInterestValue"))
            if v is None:
                v = _f(oi_obj.get("openInterestAmount"))
                if v is not None and mark:
                    cs = _f(market.get("contractSize")) or 1.0
                    v = v * cs * mark
            oi_usd = v

        return _row(
            ts_ms=ts_ms, exchange=exchange,
            symbol=_canonical_symbol(market),
            funding_rate=_f(f.get("fundingRate")),
            interval_h=_interval_h(f, info_t),
            next_ts=f.get("nextFundingTimestamp") or f.get("fundingTimestamp"),
            mark=mark, index=index, oi_usd=oi_usd,
            vol_usd=_extract_vol_usd(t, info_t, vol_info_field, mark, market),
        )

    results = await asyncio.gather(*(_one(s, m, t) for s, m, t in targets))
    return [r for r in results if r is not None]


async def collect_kucoin(c, ts_ms):
    return await _collect_with_funding_fanout(
        c, "KUCOIN", ts_ms, oi_strategy="batch",
    )


async def collect_xt(c, ts_ms):
    # XT ticker.info uses single-letter keys: m=mark, i=index. unified.
    # quoteVolume already carries the USD turnover.
    return await _collect_with_funding_fanout(
        c, "XT.COM", ts_ms, oi_strategy="skip",
        mark_info_field="m", index_info_field="i",
    )


COLLECTORS = {
    "BINANCE":  collect_binance,
    "BINGX":    collect_bingx,
    "BITGET":   collect_bitget,
    "BITMART":  collect_bitmart,
    "BYBIT":    collect_bybit,
    "COINEX":   collect_coinex,
    "GATE.IO":  collect_gate,
    "HTX":      collect_htx,
    "KUCOIN":   collect_kucoin,
    "MEXC":     collect_mexc,
    "OKX":      collect_okx,
    "PHEMEX":   collect_phemex,
    "XT.COM":   collect_xt,
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
            log.error("[%s] load_markets failed: %s: %s",
                      canonical, type(e).__name__, str(e)[:200])
            return
        log.info("[%s] markets loaded (%d total)", canonical, len(c.markets))
        while not stop.is_set():
            cycle_start = time.monotonic()
            ts_ms = int(time.time() * 1000)
            try:
                rows = await collector(c, ts_ms)
                _write_partition(canonical, ts_ms, rows)
                log.info("[%s] cycle ok: %d rows in %.1fs",
                         canonical, len(rows), time.monotonic() - cycle_start)
            except Exception as e:
                log.error("[%s] cycle failed: %s: %s",
                          canonical, type(e).__name__, str(e)[:200])
            cycles += 1
            if max_cycles > 0 and cycles >= max_cycles:
                return
            elapsed = time.monotonic() - cycle_start
            with suppress(asyncio.TimeoutError):
                await asyncio.wait_for(stop.wait(),
                                       timeout=max(0.0, POLL_INTERVAL_S - elapsed))


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
            pass  # Windows: rely on KeyboardInterrupt

    venues = list(COLLECTORS) if not args.venues else args.venues
    log.info("starting %d venues: %s (cycles=%s)",
             len(venues), ", ".join(venues),
             args.cycles if args.cycles > 0 else "inf")
    log.info("data dir: %s", FUNDING_DIR.resolve())

    tasks = [asyncio.create_task(venue_loop(v, stop, args.cycles)) for v in venues]
    await asyncio.gather(*tasks)
    log.info("collector stopped")


def main():
    p = argparse.ArgumentParser(description="Funding/OI/volume collector")
    p.add_argument("--once", action="store_true",
                   help="Shortcut for --cycles 1.")
    p.add_argument("--cycles", type=int, default=-1,
                   help="Run N cycles per venue and exit (-1=run forever).")
    p.add_argument("--venues", nargs="*", default=None,
                   help="Subset of venues to run (default: all configured).")
    args = p.parse_args()
    if args.once:
        args.cycles = 1
    try:
        asyncio.run(amain(args))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
