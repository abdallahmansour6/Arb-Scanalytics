"""Project-level constants and the CCXT client factory."""

from contextlib import asynccontextmanager
from pathlib import Path

import aiohttp
import ccxt.async_support as ccxt

PROJECT_ROOT = Path(__file__).parent
DATA_DIR = PROJECT_ROOT / "data"
FUNDING_DIR = DATA_DIR / "funding"

POLL_INTERVAL_S = 60
CCXT_TIMEOUT_MS = 30_000  # NordVPN egress adds latency; default 10s is tight.

# Canonical venue slug -> CCXT class id.
# Locked at the collector boundary; downstream layers only see the slug.
# Class-id quirks documented in FIELD_NOTES.md ("Venue -> CCXT mapping").
VENUES = {
    "BINANCE":  "binance",
    "BINGX":    "bingx",
    "BITGET":   "bitget",
    "BITMART":  "bitmart",
    "BYBIT":    "bybit",
    "COINEX":   "coinex",
    "GATE.IO":  "gate",            # not "gateio"
    "HTX":      "htx",             # not "huobi"
    "KUCOIN":   "kucoinfutures",   # separate class; no defaultType
    "MEXC":     "mexc",
    "OKX":      "okx",
    "PHEMEX":   "phemex",
    "XT.COM":   "xt",
}


def _ccxt_options(canonical: str) -> dict:
    """kucoinfutures is its own class so it does not need defaultType=swap."""
    base = {"enableRateLimit": True, "timeout": CCXT_TIMEOUT_MS}
    if canonical == "KUCOIN":
        return base
    return {**base, "options": {"defaultType": "swap"}}


@asynccontextmanager
async def open_client(canonical: str):
    """Yield a CCXT async client backed by a session that uses
    aiohttp.ThreadedResolver. aiodns (aiohttp's default when installed) fails
    intermittently under VPN/proxy on Windows with 'Timeout while contacting
    DNS servers'; the threaded resolver delegates to the OS, which honors
    the VPN's pushed DNS reliably."""
    klass = getattr(ccxt, VENUES[canonical])
    connector = aiohttp.TCPConnector(resolver=aiohttp.ThreadedResolver(),
                                     ttl_dns_cache=300)
    session = aiohttp.ClientSession(connector=connector, trust_env=True)
    opts = {**_ccxt_options(canonical), "session": session}
    client = klass(opts)
    try:
        yield client
    finally:
        await client.close()
        await session.close()
