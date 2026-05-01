# Field Notes — 13-Venue USDT-Linear Perp Scanner

Observations gathered building a funding-rate scanner across 13
USDT-linear perp venues. **Factual only**, written to spare a future
iteration the cost of rediscovery. No prescriptions — design choices
left open.

Scope assumed throughout: USDT-margined linear perpetual swaps, public
REST endpoints, polling cadence on the order of 60 s.

---

## Project venues

13 venues: BINANCE, BINGX, BITGET, BITMART, BYBIT, COINEX, GATE.IO, HTX,
KUCOIN, MEXC, OKX, PHEMEX, XT.COM. **BLOFIN was scoped out** of the
project — its API sits behind a Cloudflare anti-VPN/anti-bot challenge
that 403s any traffic flagged as VPN/proxy egress, and the project chose
to drop rather than carry the variability.

---

## Funding-rate semantics — read this first

The most expensive thing to misunderstand. Venues expose up to **three**
distinct rate concepts, which their UIs label inconsistently. "Funding
Rate", "Expected Rate", and "Predicted Rate" each mean different things
on different venues. Reduce everything to:

- **(A) Last-settled** — already-paid rate from the previous boundary.
  Frozen until next settlement. Only **BITMART** exposes a dedicated
  field for it (`info.funding_rate`).
- **(B) Upcoming** — what will settle at the *next* boundary; refines
  continuously as the TWAP-of-premium-index accumulates over the cycle.
  This is what a position opened *now* will pay/receive at next_funding_ts.
  **CCXT's unified `fundingRate` always points to (B).**
- **(C) Forward forecast** — exchange's prediction for the cycle AFTER
  upcoming. The "real" predicted-rate concept.

The label ambiguity in the wild:
- BITMART `info.expected_funding_rate` = (B) **upcoming**, NOT (C).
  Confirmed by CCXT mapping it to unified.fundingRate.
- COINEX `info.next_funding_rate` = (C) forward forecast.
- HTX `info.estimated_rate` = (C) forward forecast (often null even when the field is present).
- OKX `info.nextFundingRate` = (C) forward forecast (often empty string `''`, not None).
- PHEMEX `info.predFundingRateRr` = (C) forward forecast.
- GATE.IO `info.funding_rate_indicative` = same value as `funding_rate`, NOT a forecast.
- All other venues: (C) is not exposed.

When adding a new venue, verify the label by comparing the value
against CCXT's unified `fundingRate` — if they match, the field is (B).

---

## Network / geo

- All venues geo-block from at least some non-Asian residential IPs
  (HTTP 451 / `ExchangeNotAvailable` from CCXT). A Singapore VPS
  reaches the whole 13-venue set cleanly.
- Local development without a VPN is impossible; iterate either on the
  VPS directly (ssh) or with a VPN whose egress is in Asia.
- **NordVPN-Singapore caveat**: PacketHub-AS-backed IPs work for the
  full 13-venue set. Latency to venue endpoints is 2–15× higher than
  direct VPS routing — bump CCXT timeout to 30 s during local iteration.
- BLOFIN specifically rejects NordVPN egress with a Cloudflare 403
  ("your IP is from one of BloFin's restricted countries or regions"),
  even though `ipinfo.io` geolocates the IP as Singapore. VPN-IP-list
  detection is the proximate cause.

---

## aiohttp / aiodns / VPN — Windows DNS gotcha

Symptom: under NordVPN on Windows, every CCXT request fails with
`ExchangeNotAvailable: ... DNS error` (root cause: `aiodns` cannot
reach its configured resolvers through the VPN tunnel). `curl` works
fine because it uses the OS resolver.

Fix: pass an `aiohttp.ClientSession` whose `TCPConnector` is built
with `resolver=aiohttp.ThreadedResolver()` to CCXT's init via the
`session` kwarg. The threaded resolver delegates to the OS, which
honors the VPN's pushed DNS reliably. Implemented in `config.open_client()`.

Benign on Linux/VPS where aiodns works fine, but the workaround is
harmless there too — no need to branch on platform.

---

## Venue → CCXT mapping

CCXT version observed: **4.5.49**.

| Canonical | CCXT class       | Constructor option        |
|-----------|------------------|---------------------------|
| BINANCE   | `binance`        | `defaultType: 'swap'`     |
| BINGX     | `bingx`          | `defaultType: 'swap'`     |
| BITGET    | `bitget`         | `defaultType: 'swap'`     |
| BITMART   | `bitmart`        | `defaultType: 'swap'`     |
| BYBIT     | `bybit`          | `defaultType: 'swap'`     |
| COINEX    | `coinex`         | `defaultType: 'swap'`     |
| GATE.IO   | `gate`           | `defaultType: 'swap'` — **not** `gateio` |
| HTX       | `htx`            | `defaultType: 'swap'` — **not** `huobi`  |
| KUCOIN    | `kucoinfutures`  | (no `defaultType` — separate class)      |
| MEXC      | `mexc`           | `defaultType: 'swap'`     |
| OKX       | `okx`            | `defaultType: 'swap'`     |
| PHEMEX    | `phemex`         | `defaultType: 'swap'`     |
| XT.COM    | `xt`             | `defaultType: 'swap'`     |

---

## CCXT capability matrix (USDT-linear perps, 4.5.49)

`fetchFundingRates` (batch, returns dict of all symbols):
- **TRUE**: binance, bingx, bitget, bybit, coinex, gate, htx, okx
- **FALSE**: bitmart, kucoinfutures, mexc, phemex, xt

`fetchOpenInterests` (batch):
- **TRUE**: htx, kucoinfutures, okx
- **FALSE**: all others

`fetchOpenInterest` (per-symbol):
- **TRUE**: binance, bingx, bitget, htx, kucoinfutures, okx, phemex
- **FALSE**: gate.io, mexc, xt
- **None**: coinex (treat as False; coinex's quirk of returning None instead of False)

`fetchFundingRate`:
- **emulated** for bybit (CCXT simulates by calling fetchFundingRates and filtering)
- **TRUE** for all others

---

## Per-venue ticker.info field map

The single most useful normalization knowledge. For most venues, one
`fetch_tickers()` call covers most fields:

| Venue   | (B) Upcoming rate       | (C) Forecast            | OI                      | OI unit   | Mark           | Index          | 24h vol (USD)        | next_funding_ts |
|---------|-------------------------|-------------------------|-------------------------|-----------|----------------|----------------|----------------------|-----------------|
| BINANCE | from batch funding      | —                       | per-symbol fanout       | base      | unified.last   | unified.last   | unified.quoteVolume  | from batch funding |
| BINGX   | from batch funding      | —                       | per-symbol fanout       | base      | unified.last   | unified.last   | unified.quoteVolume  | from batch funding |
| BITGET  | `fundingRate` (info)    | —                       | `holdingAmount`         | base      | `markPrice`    | `indexPrice`   | `usdtVolume`         | from batch funding |
| BITMART | `expected_funding_rate` | —                       | `open_interest_value`   | usd       | unified.last   | `index_price`  | `turnover_24h`       | `funding_time`  |
| BYBIT   | `fundingRate` (info)    | —                       | `openInterestValue`     | usd       | `markPrice`    | `indexPrice`   | `turnover24h`        | `nextFundingTime` |
| COINEX  | from batch funding      | `next_funding_rate` (in batch info) | `open_interest_volume` | base | `mark_price`  | `index_price`  | `value`              | from batch funding |
| GATE.IO | `funding_rate` (info)   | —                       | `total_size`            | contracts | `mark_price`   | `index_price`  | `volume_24h_quote`   | from batch funding |
| HTX     | from batch funding      | `estimated_rate` (in batch info; often null) | from batch OI | — | unified.last | unified.last | `trade_turnover`*    | from batch funding |
| KUCOIN  | per-symbol fanout       | —                       | from batch OI           | —         | unified.last   | unified.last   | unified.quoteVolume  | from per-symbol funding |
| MEXC    | `fundingRate` (info)    | —                       | `holdVol`               | contracts | `fairPrice`    | `indexPrice`   | `amount24`           | (heuristic from interval) |
| OKX     | from batch funding      | `nextFundingRate` (in batch info; often `''`) | from batch OI | — | unified.last | unified.last | (compute baseVol*last)** | from batch funding |
| PHEMEX  | `fundingRateRr`         | `predFundingRateRr`     | `openInterestRv`        | base      | `markPriceRp`  | `indexPriceRp` | `turnoverRv`         | (heuristic from interval) |
| XT.COM  | per-symbol fanout       | —                       | (not exposed)           | —         | `m`            | `i`            | unified.quoteVolume  | from per-symbol funding |

\* HTX's `unified.quoteVolume` is wrong (= baseVolume × 1000). Use `info.trade_turnover` for true USD turnover.
\** OKX's `unified.quoteVolume` is None; `unified.baseVolume` is in raw contracts. Compute USD volume as `baseVolume × contractSize × mark`.

OI unit conversion to USD:
- `usd` — already USD, use as-is
- `base` — multiply by mark_price
- `contracts` — multiply by `market.contractSize × mark_price`

---

## Market-filter quirks

- **Filter must require `quote == 'USDT' AND settle == 'USDT'`** (NOT
  `OR`). The `OR` form (suggested by an earlier field-notes pass) lets
  in USDC-quoted and USD-quoted variants of the same base coin on
  bitmart (3 BTC variants: USDT, USD, USDC) and coinex (2 BTC variants:
  USDT, USDC), creating phantom duplicates per venue under canonical
  naming.
- **An older note claimed coinex's BTC perp lives at `BTC/USD:USDT`**
  (quote=USD, settle=USDT). As of CCXT 4.5.49, **coinex offers
  `BTC/USDT:USDT` natively**; the `/USD:USDT` line is either retired
  or moved to a different listing class. Tightening to AND keeps coinex's
  primary perp.
- `coinex` markets have `active=None` instead of `True` (yes, None,
  not False). Filter with `m.get('active') is False` to exclude — None
  passes.

---

## Funding interval

Sources, in priority order:
1. CCXT unified `interval` field as a string like `"8h"`, `"4h"`, `"1h"` (most reliable when present)
2. Compute `(nextFundingTimestamp − fundingTimestamp) / 3_600_000`
3. Read it from `ticker.info`: `funding_interval_hours` (bitmart),
   `fundingIntervalHour` (bybit), `collectCycle` (mexc),
   `collectionInternal` (xt)
4. Default 8 h (correct for most pairs; some run 4 h or 1 h)

### next_funding_ts heuristic

When a venue doesn't expose next_funding_ts directly (currently only
MEXC and PHEMEX, which lack a usable per-pair settlement timestamp in
ticker.info), round the current timestamp UP to the next interval
boundary, assuming UTC-aligned cycles. Verified empirically against 12
of 13 venues — all 8 h cycles align to the same UTC-anchored grid
(their nextFundingTimestamp values match across venues for the same
symbol, modulo small clock skew). 1 h and 4 h cycles also sampled
UTC-aligned. **If a venue offsets boundaries from UTC (rare, none seen
in this set), the heuristic is wrong by the offset.**

---

## Native batch endpoints (research-layer reference)

For venues without batch `fetchFundingRates`, native HTTP endpoints
return everything in one call. **Mostly unnecessary for the scanner**:
bitmart, mexc, and phemex all expose funding rate (B) and supporting
fields in their `ticker.info`, so a single `fetch_tickers()` covers
the scanner's per-cycle needs. These endpoints are still useful for
research-layer per-symbol metadata work.

### bitmart — `GET https://api-cloud-v2.bitmart.com/contract/public/details`
- v1 host (`api-cloud.bitmart.com`) returns 404; use v2.
- Carries `funding_rate` (= last-settled, A), `expected_funding_rate`
  (= upcoming, B), `next_funding_rate_timestamp`, `funding_interval_hours`,
  `last_price`, `index_price`, `open_interest_value` (USD direct),
  `turnover_24h` (USD).
- Filter `product_type == 1` and `quote_currency == 'USDT'`.
- Symbol mapping: `BTCUSDT` (no slash) → CCXT unified via `client.markets_by_id`.

### phemex — `GET https://api.phemex.com/md/v3/ticker/24hr/all`
- Response under `result[]` (not `data.result`).
- Funding rate: `fundingRateRr` (Rr = "real rate", already descaled).
  Predicted: `predFundingRateRr`.
- Mark/OI fields use scaled `Ep`/`Ev` integers needing per-market
  scaling factors. The Rr/Rp/Rv fields used in the scanner are
  already descaled — empirically verified.

### mexc — `GET https://contract.mexc.com/api/v1/contract/funding_rate`
- Response: `data: [{ symbol, fundingRate, collectCycle, nextSettleTime, ... }]`
- `collectCycle` is funding interval in hours.
- The one reason to use this from the scanner: `nextSettleTime` for all
  symbols in one call, avoiding the UTC-aligned heuristic for MEXC.

---

## CCXT 4.5.49 quirks

- **Default `timeout` (10 s) is too tight under NordVPN egress.** Bumped
  to 30 s in `config.CCXT_TIMEOUT_MS`. On a Singapore VPS direct, 10 s
  is plenty.
- **aiodns DNS failures under VPN on Windows** (see earlier section). Fix: ThreadedResolver.
- `client.fetch(url, 'GET')` crashed bitmart with `'NoneType' has no
  attribute 'lower'` from inside ccxt's header preparation. Workaround:
  raw `aiohttp.ClientSession` with explicit `User-Agent` header,
  bypassing `client.fetch`.
- Custom auto-generated method names like `client.publicContractGetDetails()`
  exist but go through the same broken `fetch`; raw aiohttp side-steps it.
- **OKX's `info.nextFundingRate` is the empty string `''`** when no
  forecast is available, not None. Coerce empty string to None before
  use.
- **HTX's `info.estimated_rate` is often null** even when the field
  exists in the response. Don't assume populated.
- **BYBIT's `c.has['fetchFundingRate'] == 'emulated'`** (string, not
  bool). CCXT simulates per-symbol via the batch call. Functionally
  equivalent for our purposes.

---

## Cycle-time realities

Per-cycle wall-clock against NordVPN-Singapore (60 s polling target):

| Group | Pattern | Venues | Per-cycle wall-clock |
|-------|---------|--------|----------------------|
| A | one fetch_tickers   | BITMART, BYBIT, MEXC, PHEMEX | 1–7 s |
| B | tickers + batch funding (± batch OI) | BITGET, COINEX, GATE.IO, HTX, OKX | 1–6 s |
| B+ | + OI per-symbol fan-out | BINANCE (~559 syms) | ~35 s |
| B+ | + OI per-symbol fan-out | BINGX (~595 syms) | **~65 s** ⚠ |
| C | tickers + funding per-symbol fan-out (± batch OI) | KUCOIN (~553 syms) | ~20 s |
| C | tickers + funding per-symbol fan-out | XT.COM (~590 syms) | **~60 s** ⚠ |

BINGX and XT.COM sit right at the 60 s budget on NordVPN. Expect 2–3×
speedup on the VPS direct route. If they consistently over-shoot on
the VPS, options: increase fan-out concurrency (currently sem=20),
bump POLL_INTERVAL_S to 90 s, or skip OI for BINANCE/BINGX (the
expensive part).

---

## Streamlit / Tornado / DuckDB

- **`use_container_width=True` is deprecated.** Replacement: `width="stretch"`
  (or `width="content"` for the False case). Applies to `st.dataframe`
  and `st.plotly_chart`.
- `st.number_input(value=None, placeholder=...)` works in Streamlit ≥ 1.30
  for "leave empty to disable" UX.
- VS Code Remote-SSH port-forward probes generate Tornado **`Invalid HTTP
  request received`** warnings into the streamlit terminal. Silence with
  `logging.getLogger("tornado.general").setLevel(logging.ERROR)`. Benign.
- VS Code Remote-SSH **auto-detects** the streamlit listening port (8501)
  and surfaces a "Open in browser" toast that opens it via the SSH
  tunnel. No manual port-forward configuration needed.
- Pandas / Streamlit dataframe sort places **NULL values at the bottom**
  regardless of asc/desc direction (`na_position='last'`).
- DuckDB INTERVAL syntax: `INTERVAL N HOUR` (no quotes around `N`).
- DuckDB does not have an `epoch_ms(timestamp)` function in 4.5.x; use
  `(epoch(now()) * 1000)::BIGINT` to get current ms-since-epoch in SQL,
  or compute it in Python.
- `read_parquet('path/**/*.parquet', hive_partitioning=true,
  union_by_name=true)` works cross-platform if forward slashes are used
  in the glob — even on Windows with backslashed paths, just `.replace("\\", "/")`.
- DuckDB `ARG_MAX(value, ts_utc)` cleanly returns "the value at the
  latest timestamp" within a `GROUP BY` — useful for "latest snapshot"
  patterns.
- **Don't name a project script `inspect.py`.** It shadows Python's
  stdlib `inspect`, causing circular-import errors when any package
  (e.g., `attr` via `aiohttp`) does `import inspect`. Use `inspector.py`
  or any other non-stdlib name.

---

## Behavioural observations on the data

- Sustained extreme funding rates **can be real**, not stale. Observed
  RLS-USDT on OKX held between −600 % and −3200 % APY (4 h interval) for
  18 hours continuously, value drifting cycle-to-cycle. Such regimes
  typically coincide with delisting / halt conditions on the venue.
- Single-cycle spike-and-revert events also occur (e.g. observed +1798 %
  APY on ST-USDT at bitmart for one cycle, back to +10 % on the next).
  Persistence filters of 2+ cycles eliminate these from anomaly views.
- **Venues vary the funding interval per-pair when funding gets extreme.**
  E.g., KUCOIN/BYBIT switch some pairs to 1 h cycles when |APY| spikes,
  effectively 8× the settlement frequency. The `apy_norm` field handles
  this transparently (`rate × 8760 / interval_h`); always compare APY,
  never raw per-epoch rates, across pairs.
- "Latest stored row per (symbol, venue)" ≠ "row from the latest cycle".
  If a venue intermittently drops a pair from its batch response, a
  `WHERE rn=1` query continues to return the stale value. Worth
  distinguishing in queries intended to reflect "right now".
- **BITMART's `info.funding_rate` is the *historical* last-settled rate**,
  not the upcoming. The upcoming is `info.expected_funding_rate`.
  Earlier code that used `info.funding_rate` recorded values one cycle
  behind reality.

---

## Probes worth re-running on a fresh iteration

The cheapest way to rebuild the OI / funding-field knowledge above on a
fresh codebase:

1. **Capability sweep** — `python probes.py capabilities`. Reads
   `client.has` for each venue. No network calls. Spots changes in
   CCXT's advertised capabilities (e.g., bybit fetchFundingRate went
   from True → "emulated" between releases).
2. **Markets sweep** — `python probes.py markets`. Counts USDT-linear
   perps per venue. Reveals new venues' listings and dead markets that
   pass the filter.
3. **Ticker info dump** — `python probes.py ticker [SYMBOL]`. For each
   venue, dumps unified ticker fields plus all `info` keys with sample
   values. Reveals OI / funding / volume / scaling fields per venue's
   raw shape. Run on a fresh CCXT release to catch field renames.
4. **Single-venue funding probe** — `python probes.py funding [SYMBOL]`.
   Per venue, dumps unified funding fields plus raw info keys plus the
   computed interval source. Confirms unified-vs-raw mapping.
5. **Pair history trace** — for any (symbol, venue) suspected of stale
   or stuck data, dump every parquet row in chronological order plus
   the gap distribution between consecutive rows. Distinguishes real
   dropouts from normal cycle intervals.
