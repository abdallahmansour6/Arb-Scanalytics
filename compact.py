"""
Two-stage compaction of the per-cycle parquet stream.

Lifecycle (each row's underlying file moves through these states once):

    per-cycle:  <VENUE>_<ms_ts>.parquet           (collector output, 30 s cadence)
                  ↓ hourly compaction (cron: 5 * * * *)
    hourly:     <VENUE>_hourly_<YYYYMMDDHH>.parquet
                  ↓ daily compaction (cron: 30 0 * * *)
    daily:      <VENUE>_daily.parquet              (final, persists indefinitely)

Why two stages
--------------
The original design only had daily compaction running at 00:30 UTC on
*yesterday's* partition, which kept the compactor safely decoupled from the
live collector and live readers — the hot, today's partition was never
touched. That worked for storage but not for *queries*: today's partition
accumulates ~37 k per-cycle files at 30 s × 13 venues, and a DuckDB view
over the wide glob walks every footer (`select max(ts_utc)` alone took
~22 s). Hourly compaction bounds the un-compacted file count on today's
partition to roughly 1 hour's worth (~1.5 k files), making dashboard reads
sub-second without touching the daily-rollup endpoint.

Race-safety on today's live partition
-------------------------------------
The hourly compactor *does* now operate on today's partition, where the
live collector is writing and the live dashboard is reading. Three
mitigations:

* Buckets are eligible only when ≥ 1 hour old (`grace_hours=1`). The
  collector never writes into a bucket that's a candidate for compaction.
* Atomic per (venue, hour): write `.tmp`, validate row count, atomic
  same-directory rename, then delete sources. A crash mid-run leaves
  source files intact and re-runs cleanly.
* Reader-side retry: `dashboard.py` wraps DuckDB reads in a one-shot
  retry on `IOException('Cannot open file ...')`. A query that races
  the rename re-enumerates and runs against the post-compaction file
  set on the second attempt.

Idempotency, schema tolerance, partial runs
-------------------------------------------
* Re-running on a (day, hour) or day that's already compacted is a no-op
  — the per-cycle / hourly source files have been deleted, so there's
  nothing left to compact for that bucket.
* If a `<VENUE>_*.parquet` target already exists *and* source files
  remain, treat as a partial-run conflict (rename done, delete didn't):
  log + skip + manual cleanup.
* Schema-change-tolerant via `concat_tables(promote_options="default")`
  — additive column changes mid-day pad older rows with NULL.

Daily fallback for missed hours
-------------------------------
If the hourly compactor misses some hours (cron skipped, transient
failure), the daily compactor at 00:30 UTC sweeps all source files in
yesterday's partition — per-cycle *and* hourly — into one daily file.
Hourly is best-effort; daily is the always-runs catch-all.

Usage
-----
    # Cron-driven (see deploy/arb-cron):
    python compact.py --mode hourly       # every hour at :05
    python compact.py --mode daily        # 00:30 UTC

    # One-off / catch-up:
    python compact.py --mode hourly --dry-run
    python compact.py --mode daily --day 2026-05-01
    python compact.py --mode hour  --day 2026-05-01 --hour 14
    python compact.py --mode day   --day 2026-05-01     # alias for --mode daily --day ...
"""

import argparse
import logging
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from config import FUNDING_DIR
from store import CYCLE_RE, venue_of_source

log = logging.getLogger("compact")


# Filename conventions live in `store.py` so dashboard.py and any future
# parquet consumer share the same shapes. We import the per-cycle regex
# directly (used by hourly bucket-membership checks) and the
# `venue_of_source` helper (used by daily compaction to identify per-cycle
# + hourly source files vs already-rolled-up daily targets).


def _partition_path(d: date) -> Path:
    return FUNDING_DIR / f"year={d.year:04d}" / f"month={d.month:02d}" / f"day={d.day:02d}"


def _atomic_compact(
    target: Path, sources: list[Path], venue: str, dry_run: bool
) -> dict:
    """Read every `sources` file, write a single compacted parquet at
    `target`, validate row count matches, atomic rename, delete sources.
    Returns a stats dict for the summary."""
    if target.exists():
        # Rename succeeded but delete didn't on a prior run, OR someone
        # added files alongside an already-compacted target. Don't risk
        # destroying data — log and skip for manual intervention.
        log.warning(
            "[%s] %s exists but %d source files remain; "
            "skipping (manual cleanup needed)",
            venue, target.name, len(sources),
        )
        return {"status": "conflict", "sources": len(sources)}

    if dry_run:
        log.info(
            "[%s] DRY RUN: would compact %d files into %s",
            venue, len(sources), target.name,
        )
        return {"status": "dry-run", "sources": len(sources)}

    temp = target.with_suffix(target.suffix + ".tmp")
    try:
        tables = [pq.read_table(s) for s in sources]
        # promote_options="default" tolerates additive schema changes
        # mid-window (e.g., a new column was added). Older rows get NULL.
        combined = pa.concat_tables(tables, promote_options="default")
        n_rows = combined.num_rows

        pq.write_table(combined, temp, compression="snappy")

        check_rows = pq.read_metadata(temp).num_rows
        if check_rows != n_rows:
            log.error(
                "[%s] row-count mismatch: wrote %d, read back %d. "
                "aborting; sources preserved.",
                venue, n_rows, check_rows,
            )
            temp.unlink(missing_ok=True)
            return {"status": "error", "sources": len(sources)}

        # Atomic on POSIX since both paths share the same directory.
        temp.rename(target)
        for s in sources:
            s.unlink()

        size_kb = target.stat().st_size / 1024
        log.info(
            "[%s] %d rows from %d files -> %s (%.1f KB)",
            venue, n_rows, len(sources), target.name, size_kb,
        )
        return {
            "status": "ok",
            "rows": n_rows,
            "sources": len(sources),
            "size_kb": size_kb,
        }
    except Exception:
        log.exception("[%s] compaction failed; sources preserved", venue)
        temp.unlink(missing_ok=True)
        return {"status": "error", "sources": len(sources)}


def compact_hour(
    year: int, month: int, day: int, hour: int, dry_run: bool = False
) -> dict[str, dict]:
    """Compact every per-cycle file falling within a single (year, month,
    day, hour) bucket into one `<VENUE>_hourly_<YYYYMMDDHH>.parquet` per
    venue. Returns {venue: stats}."""
    partition = _partition_path(date(year, month, day))
    if not partition.exists():
        return {}

    bucket_start_ms = int(
        datetime(year, month, day, hour, tzinfo=timezone.utc).timestamp() * 1000
    )
    bucket_end_ms = bucket_start_ms + 3_600_000

    by_venue: dict[str, list[Path]] = {}
    for f in sorted(partition.glob("*.parquet")):
        m = CYCLE_RE.match(f.name)
        if not m:
            continue  # already-compacted hourly/daily files are skipped
        ts_ms = int(m.group(2))
        if bucket_start_ms <= ts_ms < bucket_end_ms:
            by_venue.setdefault(m.group(1), []).append(f)

    if not by_venue:
        return {}

    bucket_id = f"{year:04d}{month:02d}{day:02d}{hour:02d}"
    return {
        venue: _atomic_compact(
            partition / f"{venue}_hourly_{bucket_id}.parquet",
            sources, venue, dry_run,
        )
        for venue, sources in by_venue.items()
    }


def compact_pending_hours(
    dry_run: bool = False, grace_hours: int = 1
) -> dict[str, dict]:
    """Find every (day, hour) bucket older than `grace_hours` that still
    has un-compacted per-cycle files, and compact each. Self-catchup: if
    the cron missed several hours, the next run consolidates everything
    pending. `grace_hours=1` ensures we never touch a bucket where the
    collector might still be writing.

    Returns flattened stats keyed by `<venue>@<YYYYMMDDHH>` so the run
    summary can iterate over a single dict."""
    now = datetime.now(timezone.utc)
    cutoff_ms = int(
        (now.replace(minute=0, second=0, microsecond=0)
         - timedelta(hours=grace_hours)).timestamp() * 1000
    )

    # Walk recent partitions (today + yesterday). Yesterday is included so
    # the 00:05 UTC run can still pick up hour-23 leftovers before the
    # 00:30 UTC daily compactor sweeps them.
    pending: set[tuple[int, int, int, int]] = set()
    for delta_days in (0, 1):
        d = (now - timedelta(days=delta_days)).date()
        partition = _partition_path(d)
        if not partition.exists():
            continue
        for f in partition.glob("*.parquet"):
            m = CYCLE_RE.match(f.name)
            if not m:
                continue
            ts_ms = int(m.group(2))
            if ts_ms >= cutoff_ms:
                continue  # too recent — let the bucket close
            file_dt = datetime.fromtimestamp(ts_ms / 1000, timezone.utc)
            pending.add((file_dt.year, file_dt.month, file_dt.day, file_dt.hour))

    if not pending:
        log.info("no pending hour buckets")
        return {}

    log.info(
        "compacting %d pending hour bucket(s): %s",
        len(pending), sorted(pending),
    )

    flat: dict[str, dict] = {}
    for (y, m, d, h) in sorted(pending):
        for venue, stats in compact_hour(y, m, d, h, dry_run=dry_run).items():
            flat[f"{venue}@{y:04d}{m:02d}{d:02d}{h:02d}"] = stats
    return flat


def compact_day(
    year: int, month: int, day: int, dry_run: bool = False
) -> dict[str, dict]:
    """Compact ALL source files in a day partition (per-cycle + hourly)
    into one `<VENUE>_daily.parquet` per venue. Returns {venue: stats}."""
    partition = _partition_path(date(year, month, day))
    if not partition.exists():
        log.info("partition %s does not exist; nothing to do", partition)
        return {}

    by_venue: dict[str, list[Path]] = {}
    for f in sorted(partition.glob("*.parquet")):
        venue = venue_of_source(f.name)
        if venue is None:
            continue  # already-compacted daily file or unrecognized
        by_venue.setdefault(venue, []).append(f)

    if not by_venue:
        log.info("partition %s already compacted", partition)
        return {}

    return {
        venue: _atomic_compact(
            partition / f"{venue}_daily.parquet",
            sources, venue, dry_run,
        )
        for venue, sources in by_venue.items()
    }


def main():
    p = argparse.ArgumentParser(description="Parquet compaction (hourly + daily)")
    p.add_argument(
        "--mode",
        choices=["hourly", "daily", "hour", "day"],
        default="daily",
        help=(
            "hourly: compact every pending hour bucket older than 1 h "
            "(default cron mode at :05). "
            "daily: compact yesterday's partition (default cron mode at 00:30). "
            "hour: compact one specific (--day, --hour). "
            "day: alias for daily with explicit --day."
        ),
    )
    p.add_argument("--day", default=None, help="UTC day YYYY-MM-DD")
    p.add_argument("--hour", type=int, default=None, help="UTC hour 0-23")
    p.add_argument("--dry-run", action="store_true",
                   help="Show what would happen; don't write or delete.")
    args = p.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    if args.mode == "hourly":
        log.info("compacting pending hour buckets%s",
                 " (DRY RUN)" if args.dry_run else "")
        stats = compact_pending_hours(dry_run=args.dry_run)
    elif args.mode == "hour":
        if not args.day or args.hour is None:
            p.error("--mode hour requires both --day and --hour")
        target = datetime.strptime(args.day, "%Y-%m-%d").date()
        log.info("compacting %s hour %02d%s", target.isoformat(), args.hour,
                 " (DRY RUN)" if args.dry_run else "")
        stats = compact_hour(
            target.year, target.month, target.day, args.hour, dry_run=args.dry_run
        )
    else:
        # daily (default cron mode) or day (explicit one-off).
        if args.day:
            target = datetime.strptime(args.day, "%Y-%m-%d").date()
        else:
            target = (datetime.now(timezone.utc) - timedelta(days=1)).date()
        log.info("compacting day %s%s", target.isoformat(),
                 " (DRY RUN)" if args.dry_run else "")
        stats = compact_day(
            target.year, target.month, target.day, dry_run=args.dry_run
        )

    if not stats:
        return

    failed = sum(1 for s in stats.values() if s.get("status") == "error")
    total_rows = sum(s.get("rows", 0) for s in stats.values())
    total_sources = sum(s.get("sources", 0) for s in stats.values())
    log.info(
        "=== %d entries, %d source files -> %d rows; failures: %d ===",
        len(stats), total_sources, total_rows, failed,
    )
    if failed:
        sys.exit(1)


if __name__ == "__main__":
    main()
