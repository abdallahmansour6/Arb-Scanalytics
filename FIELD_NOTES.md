# Field Notes — 13-Venue USDT-Linear Perp Scanner

Verified observations. Every claim below was directly probed against
live CCXT 4.5.49 endpoints; nothing is carried over from prior versions
on hearsay. The single source of truth for the per-venue extraction
spec is the table below; the collector implements it 1:1 in
`collector.py`.

Scope: USDT-quoted, USDT-settled, linear, swap markets only. REST
polling at ~60 s cadence.

---

## Project venues (13)

`BINANCE`, `BINGX`, `BITGET`, `BITMART`, `BYBIT`, `COINEX`, `GATE.IO`,
`HTX`, `KUCOIN`, `MEXC`, `OKX`, `PHEMEX`, `XT.COM`. BLOFIN was scoped
out — its API sits behind a Cloudflare anti-VPN/anti-bot challenge that
403s NordVPN-Singapore traffic.

---

## Funding-rate semantics — three concepts, two stored

Venues label these three concepts inconsistently. Reduce every venue's
docs to one of:

- **(A) Last-settled** — already-paid rate from the previous boundary.
  Frozen until next settlement. **Not stored.** Only BITMART exposes
  it as a separate field (`info.funding_rate`).
- **(B) Upcoming** — what will settle at next_funding_ts; refines as
  the TWAP-of-premium-index accumulates. CCXT's unified `fundingRate`
  always points here. **Stored as `funding_rate`.**
- **(C) Forward forecast** — exchange's forecast for the cycle AFTER
  the upcoming one. **Stored as `predicted_rate`.** Per audit, only
  COINEX, KUCOIN (when populated), HTX, OKX, PHEMEX expose this.

When wiring a new venue, always verify the field's semantic by comparing
its value to CCXT's unified `fundingRate` — match = (B), forward-shifted
value = (C).

The **most expensive field-naming trap**: BITMART's `info.funding_rate`
is the **historical last-settled rate (A)**, NOT what should go into
`funding_rate`. Use `info.expected_funding_rate` for the upcoming-cycle
rate (B). CCXT confirms this mapping by populating unified.fundingRate
from `expected_funding_rate`, not from `funding_rate`.

---

## Price fields — three columns, decreasing precision

In decreasing order of precision for basis-bps math:

- **`mark_price`** — venue's published mark price (the index-anchored
  reference used for funding settlement, liquidations, and unrealized
  PnL). NULL where the venue does not publish it: **BITMART, HTX, OKX**
  (and ~0.3% of BITMART rows where the field happens to be empty for
  some pairs). Always prefer this for basis math when populated.
- **`index_price`** — venue's published index price (multi-spot
  composite). NULL where not exposed: **HTX, OKX**. Tracks mark very
  closely.
- **`last_price`** — most recent trade price on the venue. **100%
  populated across all 13 venues**. Diverges from mark by typically
  sub-bp in calm markets, wider during fast moves; that divergence
  propagates 1:1 into any basis-bps estimate derived from it.

Stored as separate primitives. The collector NEVER overwrites
`mark_price` with `last`; that anti-pattern was the bug we removed
in the audit refactor. Downstream queries explicitly choose
`coalesce(mark_price, last_price)` and reason about the precision
tradeoff — for BITMART/HTX/OKX, the fallback to `last` is the only
way to get a per-venue price for cross-venue basis-bps work, and the
mark-last divergence is empirically sub-bp for liquid pairs.

The execution engine uses real-time L2 VWAP, not these fields. These
columns are for **scanner-level estimation** of basis-bps cost going
into the trade-viability calc:

  `basis_bps_estimate = (price_short - price_long) / mid_price * 10000`

with prices = `coalesce(mark_price, last_price)` per leg. If the result
is at the edge of viability, fire the engine to get a real-time L2 read
before committing.

---

## Per-venue extraction spec (audited 2026-05-02 against CCXT 4.5.49)

The single source of truth for `collector.py`. Every field has **one**
explicit source per venue. There are NO cascading fallbacks. Where a
venue does not expose a field, the column is NULL.

| Venue   | funding_rate (B)                      | interval_h source                                   | next_funding_ts                              | predicted (C)                  | mark                       | index                      | last                  | OI                                                    | volume_usd                                |
|---------|---------------------------------------|-----------------------------------------------------|----------------------------------------------|--------------------------------|----------------------------|----------------------------|-----------------------|-------------------------------------------------------|-------------------------------------------|
| BINANCE | batch f.unified.fundingRate           | `fapiPublicGetFundingInfo[sym].fundingIntervalHours` | f.info.nextFundingTime                       | NULL                           | f.info.markPrice           | f.info.indexPrice          | t.unified.last        | per-sym `openInterestAmount × cs × mark`              | t.unified.quoteVolume                     |
| BINGX   | batch f.unified.fundingRate           | f.info.fundingIntervalHours                         | f.info.nextFundingTime                       | NULL                           | f.info.markPrice           | f.info.indexPrice          | t.unified.last        | per-sym `openInterestValue` (USD direct)              | t.unified.quoteVolume                     |
| BITGET  | t.info.fundingRate                    | **market.info.fundInterval** (loaded once)          | UTC-derived from interval                    | NULL                           | t.info.markPrice           | t.info.indexPrice          | t.unified.last        | t.info.holdingAmount × mark                           | t.info.usdtVolume                         |
| BITMART | t.info.expected_funding_rate          | t.info.funding_interval_hours                       | t.info.funding_time                          | NULL                           | NULL (not exposed)         | t.info.index_price         | t.unified.last        | t.info.open_interest_value (USD direct)               | t.info.turnover_24h                       |
| BYBIT   | t.info.fundingRate                    | t.info.fundingIntervalHour                          | t.info.nextFundingTime                       | NULL                           | t.info.markPrice           | t.info.indexPrice          | t.unified.last        | t.info.openInterestValue (USD direct)                 | t.info.turnover24h                        |
| COINEX  | batch f.unified.fundingRate           | batch f.unified.interval ("8h" → 8.0)               | **batch f.unified.fundingTimestamp** †       | f.info.next_funding_rate       | t.info.mark_price          | t.info.index_price         | t.unified.last        | t.info.open_interest_volume × mark                    | t.info.value                              |
| GATE.IO | batch f.unified.fundingRate           | batch f.unified.interval                            | batch f.unified.fundingTimestamp             | NULL ‡                         | t.info.mark_price          | t.info.index_price         | t.unified.last        | t.info.total_size × cs × mark                         | t.info.volume_24h_quote                   |
| HTX     | batch f.unified.fundingRate           | **market.info.settlement_period** (loaded once)     | f.info.next_funding_time                     | f.info.estimated_rate          | NULL (not exposed)         | NULL (not exposed)         | t.unified.last        | batch fetch_open_interests → openInterestValue        | t.info.trade_turnover §                   |
| KUCOIN  | t.info.fundingFeeRate (batch ticker)  | t.info.fundingRateGranularity (**ms ÷ 3.6e6**)      | t.info.nextFundingRateDateTime ¶             | t.info.predictedFundingFeeRate | t.info.markPrice           | t.info.indexPrice          | t.unified.last        | t.info.openInterest × cs × mark                       | t.info.turnoverOf24h                      |
| MEXC    | native HTTP fundingRate ‖             | native HTTP collectCycle ‖                          | native HTTP nextSettleTime ‖                 | NULL                           | t.info.fairPrice           | t.info.indexPrice          | t.unified.last        | t.info.holdVol × cs × mark                            | t.info.amount24                           |
| OKX     | batch f.unified.fundingRate           | batch f.unified.interval                            | **batch f.unified.fundingTimestamp** †       | f.info.nextFundingRate \*\*    | NULL (not exposed)         | NULL (not exposed)         | t.unified.last        | batch fetch_open_interests → openInterestValue        | t.unified.baseVolume × cs × t.unified.last ⁂ |
| PHEMEX  | t.info.fundingRateRr                  | **market.info.fundingInterval** (sec ÷ 3600)        | UTC-derived from interval                    | t.info.predFundingRateRr       | t.info.markPriceRp         | t.info.indexPriceRp        | t.unified.last        | t.info.openInterestRv × mark                          | t.info.turnoverRv                         |
| XT.COM  | per-sym f.unified.fundingRate         | per-sym f.info.collectionInternal ◊                 | per-sym f.info.nextCollectionTime            | NULL                           | t.info.m                   | t.info.i                   | t.unified.last        | NULL (not exposed)                                    | t.unified.quoteVolume                     |

Footnotes:
- **†** CCXT's `unified.fundingTimestamp` is the *upcoming* boundary. Its
  `unified.nextFundingTimestamp` is the cycle AFTER upcoming — wrong
  field for our `next_funding_ts`. Verified for COINEX and OKX.
- **‡** GATE.IO's `funding_rate_indicative` is identical in value to
  `funding_rate`, NOT a forward forecast. Don't use it for predicted_rate.
- **§** HTX's `unified.quoteVolume` returns the wrong number (it equals
  `baseVolume × 1000`). `info.trade_turnover` is the actual USD volume.
- **¶** KUCOIN's `nextFundingRateTime` is a **countdown duration** (ms
  remaining), NOT an absolute timestamp. The absolute ms timestamp is
  in `nextFundingRateDateTime` (despite the misleading name).
- **‖** MEXC has no `fetchFundingRates` in CCXT; its native HTTP
  `https://contract.mexc.com/api/v1/contract/funding_rate` returns
  `fundingRate / collectCycle / nextSettleTime` for every symbol in
  one call. Cheap and complete — use it instead of per-symbol fan-out.
- **\*\*** OKX returns `info.nextFundingRate = ''` (empty string) when
  the venue hasn't computed a forecast. Coerce empty string to None.
- **⁂** OKX exposes neither mark nor index via `fetch_ticker`, and
  `quoteVolume` is null. The only available USD-volume derivation is
  `baseVolume × contractSize × last`. `last` is a trade price not the
  mark, but the difference is sub-bp and only used for vol_usd; the
  schema's `mark_price` and `index_price` columns stay NULL.
- **◊** Despite the misspelling, XT.COM's `collectionInternal` field
  carries the funding interval in hours.

UTC-aligned next_funding_ts derivation is the only allowed *math
derivation* (not fake fallback): when a venue exposes the interval but
not a per-symbol next-settlement timestamp, we round the current ts up
to the next interval boundary. Verified against 12+ venues at 8h/4h/1h
cycles — every venue aligns boundaries to the UTC grid for sampled
symbols. Currently used for BITGET and PHEMEX only.

OI unit conversions:
- "USD direct"  : as-is
- "base × mark" : multiply by mark_price (where mark is the venue-
                  authoritative mark; if mark_price is NULL the row's
                  OI is also NULL — never substitute `last`)
- "contracts × cs × mark" : multiply by `market.contractSize × mark`

Volume unit:
- All `volume_24h_usd` values stored are USD, derived per the table.

---

## CCXT capability matrix (4.5.49, audited)

`fetchFundingRates` (batch):
- TRUE: BINANCE, BINGX, BITGET, BYBIT, COINEX, GATE.IO, HTX, OKX
- FALSE: BITMART, KUCOIN, MEXC, PHEMEX, XT.COM

`fetchOpenInterests` (batch):
- TRUE: HTX, KUCOIN, OKX
- FALSE: all others

`fetchOpenInterest` (per-symbol):
- TRUE: BINANCE, BINGX, BITGET, HTX, KUCOIN, OKX, PHEMEX
- FALSE: GATE.IO, MEXC, XT.COM
- None: COINEX (treat as False — coinex's quirk of returning None)

`fetchFundingRate`:
- "emulated" for BYBIT (CCXT simulates via batch + filter)
- TRUE for the rest

---

## Venue → CCXT class id

| Canonical | CCXT class       | Constructor option                          |
|-----------|------------------|---------------------------------------------|
| BINANCE   | `binance`        | `defaultType: 'swap'`                       |
| BINGX     | `bingx`          | `defaultType: 'swap'`                       |
| BITGET    | `bitget`         | `defaultType: 'swap'`                       |
| BITMART   | `bitmart`        | `defaultType: 'swap'`                       |
| BYBIT     | `bybit`          | `defaultType: 'swap'`                       |
| COINEX    | `coinex`         | `defaultType: 'swap'`                       |
| GATE.IO   | `gate`           | `defaultType: 'swap'` — **not** `gateio`    |
| HTX       | `htx`            | `defaultType: 'swap'` — **not** `huobi`     |
| KUCOIN    | `kucoinfutures`  | (no `defaultType` — separate class)         |
| MEXC      | `mexc`           | `defaultType: 'swap'`                       |
| OKX       | `okx`            | `defaultType: 'swap'`                       |
| PHEMEX    | `phemex`         | `defaultType: 'swap'`                       |
| XT.COM    | `xt`             | `defaultType: 'swap'`                       |

---

## Network / DNS gotchas

**Geo-block**. Every venue 451s/`ExchangeNotAvailable`s some non-Asian
residential IPs. A Singapore VPS reaches the 13-venue set cleanly.
NordVPN-Singapore reaches all 13 too, with ~2–15× the latency of direct
VPS routing — bump CCXT timeout to 30 s during local iteration.

**Windows + aiohttp + aiodns + VPN**. Symptom: every CCXT request fails
with `ExchangeNotAvailable: ... DNS error`. Root cause: aiodns can't
resolve through the VPN tunnel. Fix in `config.open_client()`: pass an
`aiohttp.ClientSession` whose connector uses `aiohttp.ThreadedResolver()`.
The threaded resolver delegates to the OS, which honors the VPN's
pushed DNS reliably. Harmless on Linux/VPS — keep the workaround unconditional.

---

## Market-filter rule

The USDT-linear perp filter MUST require both `quote == 'USDT'` AND
`settle == 'USDT'`. The `OR` form (suggested by an older, unverified
field-notes pass) lets in USDC- and USD-quoted variants on bitmart and
coinex, creating phantom duplicates per venue when canonicalizing by
symbol. Verified: tightening to AND drops 24 dropouts on BITMART, 16 on
COINEX, no other venues affected.

`coinex` quirks: markets have `active=None` instead of `True` (treat
None as pass; only `False` as exclude).

---

## Cycle-time realities (NordVPN-Singapore, audited)

Per-cycle wall-clock; will be 2–3× faster from the Singapore VPS.

| Group | Pattern                                       | Venues                          | Wall-clock |
|-------|-----------------------------------------------|---------------------------------|-----------:|
| A     | one fetch_tickers + market.info               | BITGET, BITMART, BYBIT, KUCOIN, PHEMEX | 1–4 s      |
| B     | tickers + batch funding (± batch OI)          | COINEX, GATE.IO, HTX, OKX        | 2–8 s      |
| B+    | + native HTTP                                 | MEXC                             | ~10–15 s   |
| B+    | + per-symbol OI fan-out (~559 syms)           | BINANCE                          | ~35–48 s   |
| B+    | + per-symbol OI fan-out (~595 syms)           | BINGX                            | ~65–73 s ⚠ |
| C     | tickers + per-symbol funding fan-out (~590)   | XT.COM                           | ~60 s ⚠    |

BINGX and XT.COM sit at the 60 s budget on NordVPN. Watch them once on
the VPS — they should drop well under budget there. If not, raise
`POLL_INTERVAL_S` to 90 s or trim concurrency on the OI fan-out.

---

## Streamlit / DuckDB caveats (verified by use)

- `use_container_width=True` is deprecated in Streamlit ≥ 1.40 →
  `width="stretch"` (or `"content"`).
- VS Code Remote-SSH port-forward probes generate Tornado
  `Invalid HTTP request received` warnings; silence with
  `logging.getLogger("tornado.general").setLevel(logging.ERROR)`.
- VS Code Remote-SSH **auto-detects** the streamlit port (8501) and
  surfaces a "Open in browser" toast through the SSH tunnel — no
  manual port-forward configuration needed.
- DuckDB INTERVAL syntax: `INTERVAL N HOUR` (no quotes).
- `read_parquet('path/**/*.parquet', hive_partitioning=true,
  union_by_name=true)` works cross-platform if forward slashes are
  used in the glob, even on Windows.
- DuckDB `qualify row_number() over (partition by … order by … desc) = 1`
  is the "latest snapshot per group" pattern.
- **Don't name a project script `inspect.py`.** It shadows Python's
  stdlib `inspect`, breaking any package that does `import inspect`
  during import-time (e.g. `attr` via `aiohttp`). Use `inspector.py`.

---

## Probes worth re-running on a fresh CCXT release

1. `python audit.py` — full per-venue per-field source dump. Re-run
   when CCXT updates or when adding a new venue. Detect schema drifts
   (field renames, batch-vs-single info-stripping changes) immediately.
2. `python probes.py capabilities` — `c.has` snapshot per venue.
3. `python probes.py markets` — USDT-linear pair counts + samples per venue.

---

## Behavioral notes (verified empirically)

- Venues vary the funding interval per-pair when funding gets extreme
  — KUCOIN/BYBIT/BITGET/etc. shift 8h-default pairs to 4h or 1h cycles
  during volatile regimes. The fix is to read each venue's per-symbol
  interval from its authoritative source on every cycle (per the spec
  table above) — never default to a venue-wide constant.
- Cross-venue boundary alignment is empirically stable: every venue's
  same-symbol next_funding_ts at any given cycle length matches to the
  ms across the 13-venue set. UTC-grid alignment is reliable for the
  derivations marked "UTC-derived" in the spec table.
- "Latest stored row per (symbol, venue)" is not always "from the
  latest cycle" — if a venue intermittently drops a pair from its
  batch response, a `qualify rn=1` query continues to return the
  prior value. Worth distinguishing in queries that need "right now".
