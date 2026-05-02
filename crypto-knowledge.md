# Crypto / CEX / CCXT — Cross-Project Knowledge

Live document. Findings, API specs, and unexpected behaviors collected
across crypto-related projects. Project-agnostic by design — anything
specific to a single project's domain logic stays in that project's
own field-notes. The point of this file is to carry forward what's
expensive to re-discover.

**Style**: descriptive, not prescriptive. Each entry states a fact or
observation, with the date verified and the version it was tested
against. Future-self should treat older entries as "true at the time"
and re-probe before acting on anything load-bearing — venue APIs,
CCXT internals, and venue listing conventions all drift.

**How to use**: append rather than overwrite. When a new audit
contradicts a past entry, leave the past entry but annotate it with the
contradicting observation and date. The historical record is sometimes
useful for understanding *why* something used to be a certain way.

---

## CCXT (cross-version observations)

### `c.has[capability]` has four states, not two
Observed against CCXT 4.5.49 (2026-05-02). A capability key may be:
- `True` — natively supported
- `False` — not supported
- `None` — ambiguous; some venues return None instead of False (COINEX
  for `fetchOpenInterest`, observed)
- `'emulated'` — CCXT simulates by combining other endpoints (BYBIT
  `fetchFundingRate` is emulated via `fetchFundingRates`)

### Batch endpoints can strip per-symbol info that single endpoints preserve
For a given venue, `fetch_X(sym)` (single-symbol) and `fetch_Xs()`
(batch) returning equivalent shapes is *not* guaranteed. Probed
2026-05-02:
- BITGET `fetch_funding_rates()` returns per-symbol entries with
  `unified.interval=None`, `fundingTimestamp=None`, and even
  `info.fundingRateInterval=None`. The single-symbol
  `fetch_funding_rate(sym)` populates all three.
- KUCOIN `fetch_ticker(sym)` returns last-trade tick data; batch
  `fetch_tickers()` returns the proper 24h ticker shape with
  `info.fundingFeeRate`, `predictedFundingFeeRate`, `markPrice`,
  `turnoverOf24h`, etc.

Lesson: probe both shapes for any new venue/version combination before
choosing a collection strategy.

### `enableRateLimit=True` ms-spacing is independent of asyncio.Semaphore size
With `enableRateLimit=True`, CCXT enforces a minimum gap of `c.rateLimit`
ms between *outgoing* requests, regardless of how many tasks a caller
has queued behind a semaphore. Concurrency budget and dispatch cadence
are independent gates. For fan-out: total wall-clock floor =
`N_calls × rateLimit_ms`.

Default `c.rateLimit` values observed 2026-05-02 (CCXT 4.5.49):
- BINANCE: 50 ms
- BINGX: 100 ms
- BITGET: 100 ms
- BYBIT: 100 ms (verify)
- XT.COM: 100 ms

These are CCXT defaults, deliberately conservative. Venue actual
public-endpoint ceilings are usually higher.

### `c.markets_by_id[id]` returns lists, not single markets
The values are `list[market]`. Multiple markets can share a venue id
(spot + swap with the same code on some venues). Always filter the
list by your shape requirements (linear/inverse/swap/spot/etc.).

### `c.options['defaultType']` differs across CCXT classes
Most futures/swap clients accept `defaultType: 'swap'` in init options.
Exception observed: `kucoinfutures` is its own class (not a flavor of
`kucoin`) and doesn't take `defaultType`.

### CCXT class IDs that aren't what you'd guess
- `gate.io` → `gate` (not `gateio`)
- `huobi` → `htx` (renamed; `huobi` no longer works)
- `kucoin futures` → `kucoinfutures` (separate class from `kucoin`)

---

## Funding-rate semantics (universal)

### Three concepts, often labeled inconsistently across venues
A given venue may expose 0–3 of:
- **(A) Last-settled** — already-paid rate from the previous boundary;
  frozen until next settlement.
- **(B) Upcoming** — rate that will settle at the next boundary;
  refines as the TWAP-of-premium-index accumulates over the cycle.
- **(C) Forward forecast** — exchange's prediction for the cycle
  *after* the upcoming one.

CCXT's unified `fundingRate` always points to (B). When wiring a new
venue, verify any in-info field's semantic by comparing its value to
unified `fundingRate`: match → (B); systematically forward-shifted →
(C); a stale historical value → (A).

Most expensive trap observed (BITMART, 2026-05-02): `info.funding_rate`
is type (A), NOT what should populate a stored "funding_rate" column.
The (B) value lives in `info.expected_funding_rate`. CCXT confirms the
mapping by sourcing unified.fundingRate from `expected_funding_rate`.

### Funding interval can vary per pair on the same venue
Venues often shift "hot" pairs (high |APY|, manipulation candidates)
to shorter cycles dynamically — typically 8h → 4h → 1h. A fixed
per-venue default for the interval silently produces wrong APYs by
a factor of N for any pair on a non-default cycle.

Per-symbol interval sources observed 2026-05-02:
- `market.info` (loaded once at startup, per-symbol metadata):
  BITGET (`fundInterval`, hours, str), PHEMEX (`fundingInterval`,
  seconds), GATE.IO (`funding_interval`, seconds), BYBIT
  (`fundingInterval`, minutes), HTX (`settlement_period`, hours, str),
  BITMART (`funding_interval_hours`, hours, int).
- Funding response info: BINGX (`fundingIntervalHours`), MEXC
  (`collectCycle`), XT.COM (`collectionInternal` — sic, typo for
  "Interval").
- CCXT-unified `fundingRate.interval` (parsed string like `"8h"`):
  BITGET (per-symbol only, batch strips), BYBIT, COINEX, GATE.IO,
  KUCOIN, OKX, XT.COM.
- Venue-specific endpoint: BINANCE has
  `fapiPublicGetFundingInfo()` returning per-symbol
  `fundingIntervalHours` for all symbols in one call.

### Funding boundaries are UTC-aligned across major venues
Empirically verified 2026-05-02 across 13 USDT-linear venues at 1h, 4h,
and 8h cycles: boundaries align to the UTC grid (00:00, 04:00, 08:00,
…). Same-symbol `nextFundingTimestamp` matches across venues that share
the interval. Useful as a derivation when a venue exposes interval but
not a per-symbol next-timestamp.

### Funding rate is a percentage, invariant under contract multiplier
For a position notional $X with rate `r` per epoch, the funding
payment is `r × X`, regardless of how the venue scales contracts.
This makes cross-multiplier-variant spread arithmetic sound (see
"Symbol naming" below).

---

## Symbol naming / multiplier prefixes

### Same underlying token can be listed under different multiplier prefixes per venue
Tokens with very small spot prices get listed under per-contract
multipliers that vary across venues. The same underlying CHEEMS may
appear as `CHEEMS` (1×), `1000CHEEMS` (1K×), `1MCHEEMS` (1M×), or
`1000000CHEEMS` (also 1M×, different naming). All four are the same
coin.

Prefix-stripping regex (verified safe against `1INCH` and similar
digits-then-letters false positives):
```python
re.compile(r"^(?:1[KM]|10{2,7})(?=[A-Za-z])", re.IGNORECASE)
```
Strips: `100`, `1000`, `10000`, `100000`, `1000000`, `10000000`, `1K`, `1M`.

Multiplier-prefixed tokens with multi-venue presence observed
2026-05-02: PEPE, SHIB, FLOKI, BONK, LUNC, BABYDOGE, CHEEMS.

### COINEX symbol-format claim is outdated
A prior-developer note from a different iteration claimed COINEX lists
under `<base>/USD:USDT` (quote=USD, settle=USDT) and that filtering by
`quote=='USDT'` would drop the venue. Probed 2026-05-02 against CCXT
4.5.49: COINEX offers `<base>/USDT:USDT` natively. Strict
`quote=='USDT' AND settle=='USDT'` works for all 13 venues in scope
and avoids phantom duplicates from USDC-quoted variants on some
venues (BITMART, COINEX).

---

## Network / DNS / Geo

### aiodns + VPN on Windows triggers DNS timeouts
Symptom: `aiohttp.ClientConnectorDNSError: ... Timeout while contacting
DNS servers`. Root cause: aiodns's c-ares backend doesn't always pick
up the VPN-pushed DNS servers on Windows. `curl` works fine because it
uses the OS resolver.

Fix: force aiohttp to use `ThreadedResolver`, which delegates DNS to
the OS:
```python
connector = aiohttp.TCPConnector(
    resolver=aiohttp.ThreadedResolver(),
    ttl_dns_cache=300,
)
session = aiohttp.ClientSession(connector=connector, trust_env=True)
```
Pass `session=session` into CCXT's init. Benign on Linux/VPS where
aiodns works fine; the workaround is harmless cross-platform.

### CCXT default `timeout=10000` is tight under VPN egress
Observed: NordVPN-Singapore added 2–15× latency vs direct VPS routing
to the same Asian venues. CCXT's 10 s default produces frequent
spurious `ExchangeNotAvailable` errors. Bumping to 30 s eliminates them.

### "Geolocates as X" ≠ "passes anti-VPN gates at X-friendly venues"
Even commercial VPN endpoints that geolocate cleanly to an allowed
region can be flagged by anti-VPN services (Cloudflare being a common
gatekeeper) and 403'd. Observed 2026-05-02: BLOFIN returned a
Cloudflare 403 with explicit "restricted region" page to NordVPN-
Singapore traffic, even though `ipinfo.io` called the IP "Singapore".
Datacenter VPS IPs with clean reputation have a much higher hit rate.

### Most Asian CEXes geo-block non-Asian residential IPs
Symptom: HTTP 451 / `ExchangeNotAvailable` from CCXT. A Singapore VPS
or an Asian VPN typically resolves it.

---

## Per-venue quirks (specific facts worth remembering)

These accreted during one project (USDT-linear scanner across 13
venues, audited 2026-05-02 vs CCXT 4.5.49). Append-only; future
projects will discover more.

### BINANCE
- Per-symbol funding interval not in standard CCXT response. Use
  `binance.fapiPublicGetFundingInfo()` — returns list of
  `{symbol, fundingIntervalHours, ...}` for all symbols in one call.
- `fetch_open_interest(sym)` returns `openInterestAmount` (base);
  `openInterestValue=None`. Multiply by `contractSize × mark` for USD.
- No batch `fetchOpenInterests`.

### BITGET
- Batch `fetch_funding_rates()` strips `interval`, timestamps, and
  `info.fundingRateInterval` per-symbol. The single-symbol endpoint
  has them.
- Per-symbol funding interval lives in `market.info.fundInterval`
  (string, hours).
- Ticker.info has funding rate, mark, index, OI (`holdingAmount`,
  base unit), USD volume (`usdtVolume`).

### BITMART
- Two funding-rate fields in ticker.info — `funding_rate` is (A)
  last-settled; `expected_funding_rate` is (B) upcoming. CCXT's
  unified.fundingRate sources from `expected_funding_rate`.
- Doesn't publish mark price anywhere CCXT-accessible.
- `info.open_interest_value` is USD direct.

### BYBIT
- `c.has['fetchFundingRate'] = 'emulated'` (CCXT simulates from batch).
- `market.info.fundingInterval` is in **minutes** (480 = 8h).
- Ticker.info carries everything: rate, intervalHour, nextFundingTime,
  mark, index, openInterestValue (USD direct), turnover24h (USD).

### COINEX
- `fetchOpenInterest` advertises as `None` in `c.has`, not `False`.
- `unified.quoteVolume = None`; use `info.value` for 24h USD turnover.
- `info.next_funding_rate` IS the (C) forward forecast.

### GATE.IO
- CCXT class is `gate`.
- `info.funding_rate_indicative` equals `funding_rate` — not a
  forecast despite the name. Don't store as predicted_rate.
- OI in contracts: multiply by `market.contractSize × mark` for USD.

### HTX
- CCXT class is `htx`, not `huobi`.
- `unified.quoteVolume` is wrong (= `baseVolume × 1000`, an artifact);
  use `info.trade_turnover` for true USD turnover.
- Doesn't publish mark or index in CCXT-accessible surfaces.
- `market.info.settlement_period` (hours, string) is the funding interval.
- `info.estimated_rate` is (C), often null even when the field is present.

### KUCOIN
- CCXT class is `kucoinfutures`, not `kucoin`.
- Single-symbol `fetch_ticker` returns tick-event data; batch
  `fetch_tickers` returns full 24h ticker including
  `info.fundingFeeRate` (B), `predictedFundingFeeRate` (C),
  `nextFundingRateTime`, `granularity` (minutes), `markPrice`,
  `indexPrice`, `turnoverOf24h`, `openInterest`. Strongly prefer batch.

### MEXC
- No batch `fetchFundingRates` in CCXT.
- Native HTTP: `https://contract.mexc.com/api/v1/contract/funding_rate`
  returns all symbols' `fundingRate` + `collectCycle` (hours) +
  `nextSettleTime` in one call. Symbol id format: `BTC_USDT`.
- Doesn't publish mark in CCXT-unified; use `ticker.info.fairPrice`.

### OKX
- Doesn't publish mark or index in CCXT-accessible ticker.
- `unified.quoteVolume = None`; `baseVolume` is in raw contracts.
  USD vol = `baseVolume × contractSize × last`.
- `info.nextFundingRate` (C) is sometimes the empty string `''` (not
  None); coerce to None before use.

### PHEMEX
- Field-name suffixes encode scaling:
  - `Ep` / `Ev` = scaled integers; need per-market scaling factors.
  - `Rr` / `Rp` / `Rv` = "Real Rate" / "Real Price" / "Real Value",
    descaled and safe for direct use.
- `market.info.fundingInterval` is in seconds (28800 = 8h).
- Doesn't publish next-funding-ts in any CCXT-accessible field for
  per-symbol funding response.

### XT.COM
- CCXT class is `xt`.
- Ticker.info uses single-letter keys: `t/s/c/h/l/a/v/o/r/i/m/bp/ap`
  (last-trade ts, symbol, close, high, low, base-vol, quote-vol, open,
  24h-change, index, mark, bid, ask).
- Funding response info uses `collectionInternal` (sic — typo for
  "Interval") for the cycle in hours.
- Doesn't expose open interest anywhere CCXT-accessible.

---

## Streamlit (dashboarding)

### Don't name a project script `inspect.py`
Shadows Python's stdlib `inspect`. Any imported package that does
`import inspect` (e.g., `attr` via `aiohttp`) hits a circular-import
error. Pick any other name.

### Auto-refresh data sections without re-rendering whole page
`@st.fragment(run_every=N)` decorates a function so it auto-reruns
every N seconds independently of the main script. Combine with
`@st.cache_data(ttl=…)` to govern data freshness. Filter widgets must
sit OUTSIDE the fragment (so typing isn't clobbered by auto-rerun);
state persists via `st.session_state` keys.

### Deprecated container width API
`use_container_width=True` is deprecated in current Streamlit.
Replacement: `width="stretch"` (or `width="content"`). Applies to
`st.dataframe`, `st.plotly_chart`, etc.

### VS Code Remote-SSH port forwarding
VS Code auto-detects Streamlit's listening port (default 8501) and
surfaces an "Open in browser" toast that opens it via the SSH tunnel.
Suppress noisy Tornado probe warnings:
```python
logging.getLogger("tornado.general").setLevel(logging.ERROR)
```

### Pandas / Streamlit dataframe sort: NULLs always at bottom
`na_position='last'` is the default and not exposed in `st.dataframe`'s
sort UI.

---

## DuckDB / Parquet

### Hive-partitioned parquet read
```python
db.sql("create or replace view f as "
       "select * from read_parquet('path/**/*.parquet', "
       "                            hive_partitioning=true, "
       "                            union_by_name=true)")
```
Forward slashes work cross-platform — on Windows, just `.replace("\\", "/")`
the path before formatting it in. `union_by_name=true` is forgiving when
the schema evolves (older files missing newer columns just produce NULL).

### Append-only `year=YYYY/month=MM/day=DD/` partition layout
One Parquet file per cycle. DuckDB picks up new files on subsequent
queries with no migration needed. New columns added to the schema show
as NULL for older rows (with `union_by_name=true`).

### `ARG_MAX(value, key)` and `ARG_MIN(value, key)`
Returns "the value at the row with the largest/smallest key" within a
GROUP BY. Useful for "latest snapshot per group" patterns.

### `qualify` for window-function filtering
DuckDB supports the `qualify` clause, which is cleaner than wrapping
in a subquery:
```sql
select * from f
qualify row_number() over (partition by k order by ts desc) = 1
```

### `INTERVAL` syntax
`INTERVAL N HOUR`, no quotes around `N`. Example:
`(SELECT MAX(ts) FROM t) - INTERVAL 1 HOUR`.

---

## Plotly

### Range selector + drag-zoom for time-axis charts
```python
fig.update_xaxes(rangeselector=dict(buttons=[
    dict(count=1, label="1h", step="hour", stepmode="backward"),
    dict(count=6, label="6h", step="hour", stepmode="backward"),
    dict(count=24, label="1d", step="hour", stepmode="backward"),
    dict(step="all", label="All"),
]))
```
Drag-to-zoom + double-click-to-reset are native plotly behaviors on
the resulting chart. `rangeslider=dict(visible=False)` hides the bottom
strip if you don't want it.

### `hovermode="x unified"` shows the x-axis label automatically
If you also include `%{x|...}` in your `hovertemplate`, the time will
appear *twice* in the tooltip. Drop `%{x}` from the template when
unified hover is on.

### `:.4%` d3-format multiplies by 100 and appends `%`
Useful for compact display of small fractions: `%{y:.4%}` formats
`-0.000034` as `-0.0034%` in tooltips.

---

## Anti-patterns observed (don't do these)

### Defaulting unknown-source fields to a "common" placeholder
e.g., defaulting funding interval to 8h when the venue doesn't expose
it. Produces silent bad data that propagates 1:1 into derived metrics
(APY off by a factor of N). Always store NULL when the authoritative
source is missing. Alert the user to the missing data and let them decide how to proceed if a plausible default exists.

### Cascading fallbacks across heterogeneous fields
e.g., `mark = info.markPrice OR unified.last`. Different concepts get
silently conflated under one column name. Each schema field should
have ONE explicit source per venue. If multiple precisions exist
(mark vs last), store them as separate columns and let downstream
queries opt in to the precision tradeoff via explicit `coalesce`.

### Trusting prior-developer notes without re-verification
APIs drift; venue conventions change; CCXT internals refactor between
versions. Notes from a prior iteration should be treated as
"hypothesis" not "fact" — re-probe before committing to an extraction
that depends on them.

### Silently swallowing exceptions and returning a default
e.g., `try: x = fetch(...) except: x = 0`. Return None, log the
exception, and let downstream filter it out. Never invent a value to
fill a hole — that hides the failure and corrupts analytics.

---

## Probing methodology (for any new venue)

1. **Capability sweep**: dump `c.has` filtered to keys mentioning the
   concepts you care about (funding, openInterest, ticker, etc.).
2. **Markets count**: `load_markets`, count symbols matching your
   shape filter (e.g., USDT-linear). Spot-check a sample.
3. **Ticker shape (single AND batch)**: dump `info` keys + values for
   one known-vanilla symbol (BTC) and one suspected-non-vanilla
   (e.g., a hot meme coin likely on a non-default funding cycle).
   Compare shapes.
4. **Funding response (single AND batch)**: same — keys + values.
5. **OI response**: per-symbol and batch where supported.
6. **`market.info`**: dump all keys for a sample symbol — venues often
   bury per-symbol metadata (interval, next-ts, fee rates) here.

For each schema field your collector needs, identify the ONE
authoritative source per venue. If two paths agree, the extraction is
verified. If only one path is available, document the assumption.

---

## Ops / tooling reminders

- Singapore VPS for direct, low-latency, geo-clean access to Asian CEX
  APIs.
- `aiohttp.ThreadedResolver` for any aiohttp work over a VPN.
- `curl -s -o /dev/null -w "%{http_code} %{time_total}s\n" URL` for
  one-line endpoint reachability + latency probes.
- `ipinfo.io/json` for quick egress-IP geolocation check.
- Streamlit dashboard via VS Code Remote-SSH: just run
  `streamlit run dashboard.py --server.headless=true` and click the
  "Open in browser" toast that appears in VS Code.
