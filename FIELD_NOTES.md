# Field Notes — 14-Venue USDT-Linear Perp Scanner

Observations gathered during a prior pass building a funding-rate scanner over 14
Asian crypto perp venues. **Factual only**, written to spare a future iteration
the cost of rediscovery. No prescriptions — design choices left open.

Scope assumed throughout: USDT-margined linear perpetual swaps, public REST
endpoints, polling cadence on the order of 60–120 s.

---

## Network / geo

- All 14 venues geo-block from at least some non-Asian residential IPs (HTTP 451 /
  `ExchangeNotAvailable` from CCXT). A Singapore VPS reaches all 14 cleanly.
- Local development without a VPN is therefore impossible; iterate either on the
  VPS directly (ssh), Or make sure the user laptop is connect to the VPN 

---

## Venue → CCXT mapping

CCXT version observed: **4.5.49**.

| Canonical | CCXT class       | Constructor option        |
|-----------|------------------|---------------------------|
| BINANCE   | `binance`        | `defaultType: 'swap'`     |
| BINGX     | `bingx`          | `defaultType: 'swap'`     |
| BITGET    | `bitget`         | `defaultType: 'swap'`     |
| BITMART   | `bitmart`        | `defaultType: 'swap'`     |
| BLOFIN    | `blofin`         | `defaultType: 'swap'`     |
| BYBIT     | `bybit`          | `defaultType: 'swap'`     |
| COINEX    | `coinex`         | `defaultType: 'swap'`     |
| GATE.IO   | `gate`           | `defaultType: 'swap'` — **not** `gateio` |
| HTX       | `htx`            | `defaultType: 'swap'` — **not** `huobi`  |
| KUCOIN    | `kucoinfutures`  | (no `defaultType` needed — separate class) |
| MEXC      | `mexc`           | `defaultType: 'swap'`     |
| OKX       | `okx`            | `defaultType: 'swap'`     |
| PHEMEX    | `phemex`         | `defaultType: 'swap'`     |
| XT.COM    | `xt`             | `defaultType: 'swap'`     |

---

## CCXT capability matrix (USDT-linear perps)

`fetchFundingRates` (batch, returns dict of all symbols' funding):
- **TRUE**: binance, bingx, bitget, bybit, coinex, gate, htx, okx
- **FALSE**: bitmart, blofin, kucoinfutures, mexc, phemex, xt

`fetchOpenInterests` (batch):
- **TRUE**: htx, kucoinfutures, okx
- **FALSE**: all others

`fetchOpenInterest` (per-symbol):
- **TRUE**: binance, bingx (verified). Others mostly TRUE except blofin and xt
  which advertise `False` and don't expose OI in any public surface.

You should and can double check this part. 
---

## Market-filter quirks

- **coinex** populates `active` as `None` instead of `True`. A filter
  `m.get('active', True)` rejects these because the key *is* present (just with
  `None` value). Treat `active is False` as exclusion criterion; `None` passes.
- **coinex** uses `BTC/USD:USDT` style: `quote='USD'`, `settle='USDT'`. A USDT-
  margined linear filter that requires `quote == 'USDT'` will reject the entire
  venue. Accept either `quote == 'USDT'` **or** `settle == 'USDT'`.

---

## Funding interval

- CCXT's funding-rate response *sometimes* includes a string `interval` field
  (e.g. `"8h"`, `"4h"`, `"1h"`); most reliable signal when present.
- Otherwise compute `(nextFundingTimestamp − fundingTimestamp) / 3_600_000`.
- Where neither is available, the safe default for these 14 venues is **8 h**
  (most pairs); some pairs run on 4 h or 1 h schedules.

These are just some findings from the previous developer's experience; a fresh pass should re-validate and look for better signals if possible.
---

## Predicted next-epoch rate (`nextFundingRate`)

- CCXT's unified parser **drops `nextFundingRate` for OKX** despite the raw API
  response carrying it. Likely true for some other venues too.
- Recover by scanning the raw `info` blob for any of:
  `nextFundingRate`, `next_funding_rate`, `expected_funding_rate`,
  `predicted_funding_rate`, `predFundingRateRr`, `predFundingRate`,
  `estFundingRate`.

---

## OI extraction from `ticker.info`

For venues without batch `fetchOpenInterests`, OI is sometimes free-piggybacked
on `fetch_tickers()` if you know the per-venue field names:

| Venue   | Field(s) in `ticker.info`                              | Unit                                  |
|---------|--------------------------------------------------------|---------------------------------------|
| bitget  | `holdingAmount`                                        | base                                  |
| bitmart | `open_interest_value` (preferred), `open_interest`     | USD direct / base                     |
| bybit   | `openInterestValue` (preferred), `openInterest`        | USD direct / base                     |
| coinex  | `open_interest_volume`                                 | base (suspected; verify against website) |
| gate    | `total_size`                                           | **contracts** — multiply by `market.contractSize × mark` |
| mexc    | `holdVol`                                              | **contracts** — multiply by `market.contractSize × mark` (early extractor without contractSize gave inflated values) |
| phemex  | `openInterestRv`                                       | base — despite `Rv` usually meaning "real *value*" elsewhere in the API |
| binance | not in ticker info; reachable only via `fetchOpenInterest(symbol)` (per-symbol) | `openInterestAmount` in base, `openInterestValue` is `None` |
| bingx   | not in ticker info; `fetchOpenInterest(symbol)` returns `openInterestValue` in USD direct | USD direct |
| blofin  | not exposed anywhere reachable                         | —                                     |
| xt      | not exposed (single-letter info keys: `t`, `s`, `c`, `h`, `l`, `a`, `v`, `o`, `r`, `i`, `m`, `bp`, `ap`; `i` is index price, `r` is 24 h change, `m` is mark — none are OI) | — |
| htx, kucoinfutures, okx | (already covered by batch `fetchOpenInterests`) | — |

---

## Native batch endpoints (faster than CCXT's per-symbol fan-out)

For venues without batch `fetchFundingRates` (bitmart, mexc, phemex), the slow
CCXT path serially fetches per-symbol; native endpoints exist:

### bitmart — full all-in-one

- `GET https://api-cloud-v2.bitmart.com/contract/public/details`
- **Note: v1 host (`api-cloud.bitmart.com`) returns 404 on this path.** v2 host is
  the documented home for Futures v2.
- Response carries everything in one round-trip:
  `funding_rate`, `expected_funding_rate`, `next_funding_rate_timestamp`,
  `funding_interval_hours`, `last_price`, `index_price`,
  `open_interest_value` (USD direct), `turnover_24h` (USD), per `data.symbols[]`.
- Filter `product_type == 1` (perpetual) and `quote_currency == 'USDT'`.
- Symbol mapping: `BTCUSDT` (no slash) → CCXT unified via `client.markets_by_id`.

### phemex — funding-only

- `GET https://api.phemex.com/md/v3/ticker/24hr/all`
- Response under `result[]` (not `data.result`).
- Funding rate field: `fundingRateRr` (`Rr` = "real rate"). Predicted: `predFundingRateRr`.
- **Mark/OI fields use scaled `Ep`/`Ev` integers** that need per-market scaling
  factors to descale — not worth the effort for the scanner's purpose. Use this
  endpoint *only* for funding rates; pull mark/OI/volume from CCXT's
  `fetch_tickers` + the OI info-extractor.
- Symbol filter: USDT-linear (`market.linear and (settle=='USDT' or quote=='USDT')`).

### mexc — funding-only

- `GET https://contract.mexc.com/api/v1/contract/funding_rate`
- Response: `data: [{ symbol, fundingRate, collectCycle, nextSettleTime, ... }]`.
- `collectCycle` is funding interval in hours.
- Symbol format: `BTC_USDT` (underscore) → CCXT unified via `markets_by_id`.
- For tickers/OI, keep using CCXT path.

---

## CCXT 4.5.49 quirks

- **`client.fetch(url, 'GET')` crashed bitmart** with `'NoneType' object has no
  attribute 'lower'` from inside ccxt's header preparation. Workaround: use a
  raw `aiohttp.ClientSession` with an explicit `User-Agent` header, bypassing
  `client.fetch`.
- Custom auto-generated method names like `client.publicContractGetDetails()`
  exist but go through the same broken `fetch`; raw aiohttp side-steps it
  cleanly.

---

## Streamlit / Tornado / DuckDB

- Streamlit: `use_container_width=True` is **deprecated**. Replacement is
  `width="stretch"` (or `width="content"` for the False case). Applies to
  `st.dataframe` and `st.plotly_chart`.
- `st.number_input(value=None, placeholder=...)` works in Streamlit ≥ 1.30 for
  "leave empty to disable" UX.
- VS Code Remote-SSH port-forward probes generate Tornado **`Invalid HTTP
  request received`** warnings into the streamlit terminal. Silence with
  `logging.getLogger("tornado.general").setLevel(logging.ERROR)`. Benign.
- Pandas / Streamlit dataframe sort places **NULL values at the bottom**
  regardless of asc/desc direction (`na_position='last'`).
- DuckDB INTERVAL syntax: `INTERVAL N HOUR` (no quotes around `N`), e.g.
  `(SELECT MAX(ts_utc) FROM funding) - INTERVAL 1 HOUR`.
- `read_parquet('path/**/*.parquet', hive_partitioning=true, union_by_name=true)`
  works cross-platform if forward slashes are used in the glob, even on Windows.
- DuckDB `ARG_MAX(value, ts_utc)` cleanly returns "the value at the latest
  timestamp" within a `GROUP BY` — useful for "latest snapshot" patterns.

---

## Funding-rate semantics (the most expensive thing to misunderstand)

Most venues display **two** funding rates on the pair page:

- **"Funding Rate"** — the rate that will settle at the **upcoming** funding
  boundary. This is what `ccxt.fetch_funding_rates()` returns as `fundingRate`.
- **"Next Funding Rate"** — a forward forecast for the cycle **after** the
  upcoming one. Maps to `nextFundingRate` (which CCXT drops for some venues —
  see Predicted Rate section above).

Both are real numbers; they can disagree dramatically during volatile periods.
Comparing the dashboard's "current accruing" against a website's "Next Funding"
display will surface a mismatch that *isn't* a bug.

For arb decisions on a position opened *now*, the first funding payment lands
at the next boundary, settled at **`fundingRate`**. Subsequent settlements use
the forward rates.

---

## Behavioural observations on the data

- Sustained extreme funding rates **can be real**, not stale. Observed RLS-USDT
  on OKX held between −600 % and −3200 % APY (4 h interval) for 18 hours
  continuously, value drifting cycle-to-cycle (so not a frozen value), confirmed
  by 100 % cycle coverage. Such regimes typically coincide with delisting / halt
  conditions on the venue.
- Single-cycle spike-and-revert events also occur (e.g. observed +1798 % APY on
  ST-USDT at bitmart for one cycle, back to +10 % on the next). Persistence
  filters of 2+ cycles eliminate these from anomaly views.
- "Latest stored row per (symbol, venue)" ≠ "row from the latest cycle". If a
  venue intermittently drops a pair from its batch response, a `WHERE rn=1`
  query continues to return the stale value. Worth distinguishing in queries
  intended to reflect "right now."

---

## Probes worth re-running on a fresh iteration

The cheapest way to rebuild the OI / funding-field knowledge above on a fresh
codebase:

1. **Capability sweep** — for each venue, instantiate a CCXT client and dump
   `client.has` filtered to keys containing `funding` or `openinterest`.
2. **Ticker info dump** — `fetch_ticker(BTC/USDT:USDT)` per venue, print all
   keys of `info` with values; reveals OI / funding / volume / scaling fields
   per venue's raw shape.
3. **Single-venue funding probe** — `fetch_funding_rate(symbol)` per venue;
   print `fundingRate`, `nextFundingRate`, `interval`, all timestamp fields,
   and `info` keys. Confirms unified-vs-raw mapping per venue.
4. **Pair history trace** — for any (symbol, venue) suspected of stale or stuck
   data, dump every parquet row in chronological order plus the gap distribution
   between consecutive rows. Distinguishes real dropouts from normal cycle
   intervals.
