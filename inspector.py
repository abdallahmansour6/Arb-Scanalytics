"""
Sanity probes against the persisted Parquet partitions.

Usage:
    python inspect.py                # run all sections
    python inspect.py overview       # row counts + coverage per venue
    python inspect.py btc            # BTC/USDT:USDT row across venues
    python inspect.py spreads        # top cross-venue funding spreads
    python inspect.py anomalies      # top |APY| anomalies (vol-gated)

Caveat (per FIELD_NOTES.md): "latest snapshot per (exchange, symbol)" via
window function returns the most recent row even if that row is stale —
when a venue intermittently drops a pair from its batch response, the
prior value persists in this view. Use the *_overview_ts_utc column to
spot freshness gaps.
"""

import sys

import duckdb

from config import FUNDING_DIR


_GLOB = str(FUNDING_DIR / "**" / "*.parquet").replace("\\", "/")


def _open():
    db = duckdb.connect()
    db.sql(f"""create or replace view f as
               select * from read_parquet('{_GLOB}', hive_partitioning=true)""")
    return db


def _print_df(df):
    print(df.to_string(index=False))


def overview():
    db = _open()
    print("\n=== overview ===")
    row = db.sql("""select count(*) as rows,
                           count(distinct exchange) as venues,
                           count(distinct symbol_canonical) as symbols,
                           count(distinct ts_utc) as ts_cycles,
                           epoch_ms(min(ts_utc)) as first_ts,
                           epoch_ms(max(ts_utc)) as last_ts
                    from f""").df()
    _print_df(row)
    print("\nper-venue:")
    _print_df(db.sql("""
        select exchange,
               count(distinct ts_utc) as cycles,
               count(*) as rows,
               count(predicted_rate) as has_pred,
               count(open_interest_usd) as has_oi,
               count(volume_24h_usd) as has_vol,
               count(next_funding_ts) as has_next_ts,
               max(epoch_ms(ts_utc)) as last_seen
        from f group by 1 order by 1
    """).df())


def btc():
    db = _open()
    print("\n=== BTC/USDT:USDT across venues (latest per venue) ===")
    # No coalesce on predicted_rate — null distinguishes venues that don't
    # expose a forward forecast (most of them) from those that do.
    _print_df(db.sql("""
        select exchange,
               round(funding_rate, 6) as rate,
               funding_interval_h as h,
               round(predicted_rate, 6) as predicted,
               round(apy_norm * 100, 2) as apy_pct,
               round(mark_price, 2) as mark,
               round(open_interest_usd / 1e6, 1) as oi_musd,
               round(volume_24h_usd / 1e6, 1) as vol_musd,
               epoch_ms(ts_utc) as ts
        from f where symbol_canonical = 'BTC/USDT:USDT'
        qualify row_number() over (partition by exchange order by ts_utc desc) = 1
        order by exchange
    """).df())


def spreads(limit: int = 20):
    db = _open()
    print(f"\n=== top {limit} cross-venue funding spreads (latest, both legs >= $1M vol) ===")
    # Per-leg + cross-multiplier refactor: short_* / long_* values come
    # from the exact two (venue, canonical) rows with the highest /
    # lowest APY for each base_coin. Grouping by base_coin (instead of
    # symbol_canonical) collapses '1000CHEEMS' / '1MCHEEMS' / '1000000CHEEMS'
    # / 'CHEEMS' onto the same logical token. Each leg row keeps its
    # actual venue-specific symbol so trades can be routed correctly.
    _print_df(db.sql(f"""
        with latest as (
            select * from f
            qualify row_number() over (partition by exchange, symbol_canonical
                                       order by ts_utc desc) = 1
        ),
        ranked as (
            select *,
                   row_number() over (partition by base_coin
                                      order by apy_norm desc, exchange) as rk_high,
                   row_number() over (partition by base_coin
                                      order by apy_norm asc,  exchange) as rk_low,
                   count(distinct exchange) over (partition by base_coin) as listings
            from latest
        ),
        shorts as (
            select base_coin, listings,
                   exchange         as venue_short,
                   symbol_canonical as short_symbol,
                   apy_norm         as short_apy,
                   volume_24h_usd   as short_vol_usd
            from ranked where rk_high = 1
        ),
        longs as (
            select base_coin,
                   exchange         as venue_long,
                   symbol_canonical as long_symbol,
                   apy_norm         as long_apy,
                   volume_24h_usd   as long_vol_usd
            from ranked where rk_low = 1
        )
        select s.base_coin,
               s.listings,
               s.venue_short, s.short_symbol,
               l.venue_long,  l.long_symbol,
               round(s.short_apy * 100, 1)                  as short_apy_pct,
               round(l.long_apy  * 100, 1)                  as long_apy_pct,
               round((s.short_apy - l.long_apy) * 100, 1)   as delta_apy_pct,
               round(s.short_vol_usd / 1e6, 1)              as short_vol_musd,
               round(l.long_vol_usd  / 1e6, 1)              as long_vol_musd
        from shorts s join longs l using (base_coin)
        where s.listings >= 2
          and s.short_vol_usd >= 1e6 and l.long_vol_usd >= 1e6
        order by delta_apy_pct desc
        limit {limit}
    """).df())


def anomalies(limit: int = 20):
    db = _open()
    print(f"\n=== top {limit} |APY| anomalies (latest, vol >= $5M) ===")
    _print_df(db.sql(f"""
        with latest as (
            select * from f
            qualify row_number() over (partition by exchange, symbol_canonical
                                       order by ts_utc desc) = 1
        )
        select exchange,
               symbol_canonical,
               round(funding_rate, 6) as rate,
               funding_interval_h as h,
               round(apy_norm * 100, 1) as apy_pct,
               round(volume_24h_usd / 1e6, 1) as vol_musd,
               round(open_interest_usd / 1e6, 1) as oi_musd
        from latest
        where volume_24h_usd >= 5e6
        order by abs(apy_norm) desc
        limit {limit}
    """).df())


MODES = {"overview": overview, "btc": btc, "spreads": spreads, "anomalies": anomalies}


def main():
    cmd = sys.argv[1] if len(sys.argv) > 1 else "all"
    if cmd == "all":
        overview(); btc(); spreads(); anomalies()
        return
    fn = MODES.get(cmd)
    if not fn:
        print(__doc__)
        sys.exit(2)
    fn()


if __name__ == "__main__":
    main()
