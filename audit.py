"""
Comprehensive per-venue, per-field source audit.

For every (venue, symbol), enumerates every candidate source for every
canonical schema field across:
  - market.info     (loaded once via load_markets)
  - ticker.info     (live, fetch_ticker)
  - funding.info    (live, fetch_funding_rate per-symbol)
  - per-symbol fetch_open_interest
  - batch fetch_open_interests (per-symbol entry)

Output is verbose by design — the goal is to read the dump and build a
verified, no-fallback, one-source-per-(venue,field) extraction spec.

Two symbols probed:
  - BTC/USDT:USDT — vanilla, default 8 h cycle on most venues
  - LAB/USDT:USDT — verified to be on a non-default cycle (4 h) on
                    several venues; flushes out the interval bug.

Run:
    python audit.py [SYMBOL ...]   # default: BTC and LAB
    python audit.py BTC/USDT:USDT  # one symbol
"""

import asyncio
import json
import sys
from typing import Any

from config import VENUES, open_client

# Field-name tokens used to scan info dicts. Casefold compared.
INTERVAL_TOKENS = ("interval", "cycle", "period")
NEXT_TS_TOKENS = (
    "nexttime",
    "next_time",
    "fundingtime",
    "funding_time",
    "settletime",
    "settle_time",
    "settle_timestamp",
    "fundinapply",
    "next_funding_apply",
    "fundingnextapply",
    "nextupdate",
    "nextcollection",
    "nextsettle",
)
PREDICTED_TOKENS = (
    "nextfundingrate",
    "next_funding_rate",
    "expected",
    "predicted",
    "predfunding",
    "estfunding",
    "estimated",
    "indicative",
)
MARK_TOKENS = ("mark", "fairprice", "fair_price")
INDEX_TOKENS = ("index",)
OI_TOKENS = (
    "openinterest",
    "open_interest",
    "holding",
    "holdvol",
    "totalsize",
    "total_size",
    "oi",
)
VOL_TOKENS = (
    "turnover",
    "volume_24h_quote",
    "volume_24h_settle",
    "amount24",
    "value",
    "usdtvolume",
    "trade_turnover",
    "quote_volume",
)


def _scan(info: dict, tokens: tuple) -> list[tuple[str, Any]]:
    """Return [(key, value)] for keys whose lowercased name contains any
    of the tokens. Excludes values that are None or empty string."""
    out = []
    for k, v in (info or {}).items():
        kl = k.lower().replace("_", "")
        for tok in tokens:
            t = tok.lower().replace("_", "")
            if t in kl:
                if v is None or v == "":
                    continue
                out.append((k, v))
                break
    return out


def _short(v):
    """Truncate long strings/dicts for terminal-friendly printing."""
    s = repr(v)
    return s if len(s) <= 80 else s[:77] + "..."


async def audit_venue_symbol(canonical: str, sym: str) -> dict:
    async with open_client(canonical) as c:
        try:
            await c.load_markets()
        except Exception as e:
            return {
                "venue": canonical,
                "symbol": sym,
                "error": f"load_markets: {type(e).__name__}: {str(e)[:100]}",
            }

        if sym not in c.markets:
            return {"venue": canonical, "symbol": sym, "error": "not listed"}

        m = c.markets[sym]
        m_info = m.get("info") or {}
        out = {
            "venue": canonical,
            "symbol": sym,
            "contract_size": m.get("contractSize"),
            "market_info_all_keys": sorted(m_info.keys()),
        }

        # Ticker
        try:
            t = await c.fetch_ticker(sym)
            t_info = t.get("info") or {}
            out["ticker_unified"] = {
                k: t.get(k)
                for k in (
                    "last",
                    "markPrice",
                    "indexPrice",
                    "bid",
                    "ask",
                    "baseVolume",
                    "quoteVolume",
                )
            }
            out["ticker_info_keys"] = sorted(t_info.keys())
        except Exception as e:
            out["ticker_error"] = f"{type(e).__name__}: {str(e)[:100]}"
            t_info = {}

        # Per-symbol funding
        try:
            f = await c.fetch_funding_rate(sym)
            f_info = f.get("info") or {}
            out["funding_unified"] = {
                k: f.get(k)
                for k in (
                    "fundingRate",
                    "nextFundingRate",
                    "interval",
                    "fundingTimestamp",
                    "nextFundingTimestamp",
                    "markPrice",
                    "indexPrice",
                )
            }
            out["funding_info_keys"] = sorted(f_info.keys())
        except Exception as e:
            out["funding_error"] = f"{type(e).__name__}: {str(e)[:100]}"
            f_info = {}

        # Per-symbol OI
        if c.has.get("fetchOpenInterest") in (True, "emulated"):
            try:
                o = await c.fetch_open_interest(sym)
                out["per_sym_oi"] = {
                    "openInterestAmount": o.get("openInterestAmount"),
                    "openInterestValue": o.get("openInterestValue"),
                }
            except Exception as e:
                out["per_sym_oi_error"] = f"{type(e).__name__}: {str(e)[:80]}"

        # Per-field source candidates — scan all three info dicts
        out["candidates"] = {
            "interval": (
                _scan(m_info, INTERVAL_TOKENS)
                + [(f"funding.info.{k}", v) for k, v in _scan(f_info, INTERVAL_TOKENS)]
                + [(f"ticker.info.{k}", v) for k, v in _scan(t_info, INTERVAL_TOKENS)]
            ),
            "next_funding_ts": (
                _scan(m_info, NEXT_TS_TOKENS)
                + [(f"funding.info.{k}", v) for k, v in _scan(f_info, NEXT_TS_TOKENS)]
                + [(f"ticker.info.{k}", v) for k, v in _scan(t_info, NEXT_TS_TOKENS)]
            ),
            "predicted_rate": (
                [(f"funding.info.{k}", v) for k, v in _scan(f_info, PREDICTED_TOKENS)]
                + [(f"ticker.info.{k}", v) for k, v in _scan(t_info, PREDICTED_TOKENS)]
            ),
            "mark_price": (
                [(f"ticker.info.{k}", v) for k, v in _scan(t_info, MARK_TOKENS)]
                + [(f"funding.info.{k}", v) for k, v in _scan(f_info, MARK_TOKENS)]
            ),
            "index_price": (
                [(f"ticker.info.{k}", v) for k, v in _scan(t_info, INDEX_TOKENS)]
                + [(f"funding.info.{k}", v) for k, v in _scan(f_info, INDEX_TOKENS)]
            ),
            "open_interest": [
                (f"ticker.info.{k}", v) for k, v in _scan(t_info, OI_TOKENS)
            ],
            "volume_24h": [
                (f"ticker.info.{k}", v) for k, v in _scan(t_info, VOL_TOKENS)
            ],
        }

        return out


def print_findings(report: dict):
    venue = report["venue"]
    sym = report["symbol"]
    print(f"\n{'='*80}")
    print(f"  {venue}  {sym}")
    print(f"{'='*80}")
    if "error" in report:
        print(f"  ERROR: {report['error']}")
        return

    print(f"  contractSize = {report.get('contract_size')}")
    print()
    print(f"  ticker.unified : {report.get('ticker_unified')}")
    print(f"  funding.unified: {report.get('funding_unified')}")
    if "per_sym_oi" in report:
        print(f"  per_sym_oi     : {report['per_sym_oi']}")

    for field, cands in report["candidates"].items():
        print(f"\n  {field} candidates ({len(cands)}):")
        if not cands:
            print(f"    (none)")
            continue
        for src, val in cands:
            print(f"    {src:50} = {_short(val)}")

    # Show full info-key inventories for completeness
    print(
        f"\n  market.info keys ({len(report.get('market_info_all_keys', []))}): {report.get('market_info_all_keys', [])}"
    )
    print(
        f"  ticker.info keys ({len(report.get('ticker_info_keys', []))}): {report.get('ticker_info_keys', [])}"
    )
    print(
        f"  funding.info keys ({len(report.get('funding_info_keys', []))}): {report.get('funding_info_keys', [])}"
    )


async def main():
    symbols = sys.argv[1:] if len(sys.argv) > 1 else ["BTC/USDT:USDT", "LAB/USDT:USDT"]
    print(f"Auditing {len(VENUES)} venues × {len(symbols)} symbols: {symbols}")
    for venue in VENUES:
        for sym in symbols:
            r = await audit_venue_symbol(venue, sym)
            print_findings(r)


if __name__ == "__main__":
    asyncio.run(main())
