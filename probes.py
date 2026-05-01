"""
Re-validate the per-venue surface from FIELD_NOTES.md against live API.

Modes:
  python probes.py capabilities          # instant; reads c.has only
  python probes.py markets               # USDT-linear perp count per venue
  python probes.py ticker [SYMBOL]       # fetch_ticker dump per venue
  python probes.py funding [SYMBOL]      # fetch_funding_rate dump per venue
  python probes.py all [SYMBOL]          # capabilities + markets + ticker + funding

Default symbol: BTC/USDT:USDT (auto-falls back to BTC/USD:USDT for coinex).

Requires Singapore-region IP. Field-notes finding: every venue geo-blocks
non-Asian residential IPs with HTTP 451 / ExchangeNotAvailable.
"""

import asyncio
import sys
from pprint import pformat

from config import VENUES, open_client


# Field-notes: candidate keys that hold the predicted/forecast next-epoch
# rate inside the raw `info` blob when CCXT's unified parser drops it.
PREDICTED_KEYS = (
    "nextFundingRate", "next_funding_rate", "expected_funding_rate",
    "predicted_funding_rate", "predFundingRateRr", "predFundingRate",
    "estFundingRate",
)


def _short(v, max_len=80):
    s = repr(v)
    return s if len(s) <= max_len else s[: max_len - 3] + "..."


def _resolve_symbol(client, requested: str) -> str | None:
    """BTC/USDT:USDT for most; BTC/USD:USDT for coinex (settle=USDT, quote=USD).
    Returns the first candidate present in client.markets, or None."""
    candidates = [requested, requested.replace("/USDT:", "/USD:")]
    for s in candidates:
        if s in client.markets:
            return s
    return None


def _is_usdt_linear(market: dict) -> bool:
    """USDT-quoted, USDT-settled, linear, swap, not deactivated. Mirrors the
    collector's filter so probe counts match what the collector ingests."""
    if not market.get("swap") or not market.get("linear"):
        return False
    if market.get("active") is False:
        return False
    return market.get("quote") == "USDT" and market.get("settle") == "USDT"


# ---------------------------------------------------------------------------
# capabilities
# ---------------------------------------------------------------------------

async def _caps_one(canonical: str) -> dict:
    async with open_client(canonical) as c:
        keys = ("fetchFundingRate", "fetchFundingRates",
                "fetchOpenInterest", "fetchOpenInterests",
                "fetchTicker", "fetchTickers")
        return {k: c.has.get(k) for k in keys}


async def capabilities():
    print("\n" + "=" * 110)
    print("CAPABILITIES (c.has)")
    print("=" * 110)
    results = await asyncio.gather(*(_caps_one(v) for v in VENUES),
                                   return_exceptions=True)
    print(f"{'venue':<10}{'fetchFundingRate':>18}{'fetchFundingRates':>20}"
          f"{'fetchOpenInterest':>20}{'fetchOpenInterests':>20}"
          f"{'fetchTicker':>14}{'fetchTickers':>14}")
    print("-" * 110)
    for canonical, r in zip(VENUES, results):
        if isinstance(r, Exception):
            print(f"{canonical:<10}  ERROR: {type(r).__name__}: {r}")
            continue
        print(f"{canonical:<10}"
              f"{str(r['fetchFundingRate']):>18}"
              f"{str(r['fetchFundingRates']):>20}"
              f"{str(r['fetchOpenInterest']):>20}"
              f"{str(r['fetchOpenInterests']):>20}"
              f"{str(r['fetchTicker']):>14}"
              f"{str(r['fetchTickers']):>14}")


# ---------------------------------------------------------------------------
# markets
# ---------------------------------------------------------------------------

async def _markets_one(canonical: str) -> dict:
    async with open_client(canonical) as c:
        await c.load_markets()
        usdt_linear = [s for s, m in c.markets.items() if _is_usdt_linear(m)]
        return {"total_markets": len(c.markets), "usdt_linear": len(usdt_linear),
                "sample": usdt_linear[:5]}


async def markets():
    print("\n" + "=" * 110)
    print("MARKETS (USDT-linear perp count per venue)")
    print("=" * 110)
    results = await asyncio.gather(*(_markets_one(v) for v in VENUES),
                                   return_exceptions=True)
    print(f"{'venue':<10}{'total':>10}{'usdt_linear':>14}  sample")
    print("-" * 110)
    for canonical, r in zip(VENUES, results):
        if isinstance(r, Exception):
            print(f"{canonical:<10}  ERROR: {type(r).__name__}: {r}")
            continue
        print(f"{canonical:<10}{r['total_markets']:>10}{r['usdt_linear']:>14}"
              f"  {r['sample']}")


# ---------------------------------------------------------------------------
# ticker
# ---------------------------------------------------------------------------

async def _ticker_one(canonical: str, symbol: str) -> dict:
    async with open_client(canonical) as c:
        await c.load_markets()
        s = _resolve_symbol(c, symbol)
        if s is None:
            return {"error": f"symbol {symbol!r} (and /USD: variant) not in markets"}
        t = await c.fetch_ticker(s)
        info = t.get("info") or {}
        market = c.markets[s]
        return {
            "symbol": s,
            "contract_size": market.get("contractSize"),
            "unified": {
                "last": t.get("last"),
                "mark": t.get("markPrice") or t.get("mark"),
                "index": t.get("indexPrice"),
                "bid": t.get("bid"),
                "ask": t.get("ask"),
                "baseVolume": t.get("baseVolume"),
                "quoteVolume": t.get("quoteVolume"),
            },
            "info_keys": sorted(info.keys()),
            "info_sample": {k: _short(info[k]) for k in sorted(info.keys())},
        }


async def ticker(symbol: str = "BTC/USDT:USDT"):
    print("\n" + "=" * 110)
    print(f"TICKER (fetch_ticker, symbol={symbol})")
    print("=" * 110)
    results = await asyncio.gather(*(_ticker_one(v, symbol) for v in VENUES),
                                   return_exceptions=True)
    for canonical, r in zip(VENUES, results):
        print(f"\n--- {canonical} ---")
        if isinstance(r, Exception):
            print(f"ERROR: {type(r).__name__}: {r}")
            continue
        print(pformat(r, width=120, sort_dicts=False))


# ---------------------------------------------------------------------------
# funding
# ---------------------------------------------------------------------------

def _interval_h(unified: dict) -> tuple[float | None, str]:
    """Return (interval_hours, source). Mirrors collector logic so we can
    verify it from the probe."""
    iv = unified.get("interval")
    if isinstance(iv, str) and iv.endswith("h"):
        try:
            return float(iv.rstrip("h")), "interval_field"
        except ValueError:
            pass
    fts = unified.get("fundingTimestamp")
    nfts = unified.get("nextFundingTimestamp")
    if fts and nfts and nfts > fts:
        return (nfts - fts) / 3_600_000, "ts_diff"
    return None, "none"


def _scan_predicted(info: dict):
    return {k: info[k] for k in PREDICTED_KEYS if k in info}


async def _funding_one(canonical: str, symbol: str) -> dict:
    async with open_client(canonical) as c:
        await c.load_markets()
        s = _resolve_symbol(c, symbol)
        if s is None:
            return {"error": f"symbol {symbol!r} (and /USD: variant) not in markets"}
        if not c.has.get("fetchFundingRate"):
            return {"symbol": s, "note": "fetchFundingRate not advertised"}
        f = await c.fetch_funding_rate(s)
        info = f.get("info") or {}
        interval_h, interval_src = _interval_h(f)
        return {
            "symbol": s,
            "fundingRate": f.get("fundingRate"),
            "nextFundingRate_unified": f.get("nextFundingRate"),
            "interval_field": f.get("interval"),
            "fundingTimestamp": f.get("fundingTimestamp"),
            "nextFundingTimestamp": f.get("nextFundingTimestamp"),
            "computed_interval_h": interval_h,
            "interval_source": interval_src,
            "predicted_in_info": _scan_predicted(info),
            "info_keys": sorted(info.keys()),
        }


async def funding(symbol: str = "BTC/USDT:USDT"):
    print("\n" + "=" * 110)
    print(f"FUNDING (fetch_funding_rate, symbol={symbol})")
    print("=" * 110)
    results = await asyncio.gather(*(_funding_one(v, symbol) for v in VENUES),
                                   return_exceptions=True)
    for canonical, r in zip(VENUES, results):
        print(f"\n--- {canonical} ---")
        if isinstance(r, Exception):
            print(f"ERROR: {type(r).__name__}: {r}")
            continue
        print(pformat(r, width=120, sort_dicts=False))


# ---------------------------------------------------------------------------
# entry
# ---------------------------------------------------------------------------

async def main():
    cmd = (sys.argv[1] if len(sys.argv) > 1 else "all").lower()
    arg = sys.argv[2] if len(sys.argv) > 2 else "BTC/USDT:USDT"
    if cmd == "capabilities":
        await capabilities()
    elif cmd == "markets":
        await markets()
    elif cmd == "ticker":
        await ticker(arg)
    elif cmd == "funding":
        await funding(arg)
    elif cmd == "all":
        await capabilities()
        await markets()
        await ticker(arg)
        await funding(arg)
    else:
        print(__doc__)
        sys.exit(2)


if __name__ == "__main__":
    asyncio.run(main())
