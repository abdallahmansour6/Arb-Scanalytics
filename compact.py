"""
Daily compaction of per-cycle parquet files into one file per venue per day.

Reduces file count from ~26,000/day (13 venues × 30 s cycle × 1440 min ÷
2 cycles/min × 13 files/cycle … hand-wave) to 13/day by merging cycle
files within each (year=, month=, day=) partition. Same rows, same
columns, same query semantics — pure storage optimization that recovers
the metadata overhead Parquet pays per-file.

Atomic per venue: write a `<venue>_daily.parquet.tmp`, validate row count
matches the sum across source files, atomically rename to
`<venue>_daily.parquet`, then delete the sources. A crash mid-run leaves
source files intact; the next run retries.

Idempotent: re-running on a day that's already been compacted is a no-op.
A surviving `_daily.parquet` for a venue plus residual sources signals
a partial-run conflict — logged as a warning, manual cleanup required.

Schema-change-tolerant: uses pyarrow `concat_tables(promote_options=
"default")`, so a mid-day schema bump (column added) merges cleanly,
padding older rows with NULL for the new column.

Usage:
    python compact.py                       # compact yesterday (UTC)
    python compact.py --day 2026-05-01      # compact a specific day
    python compact.py --day 2026-05-01 --dry-run

Cron (00:30 UTC daily):
    30 0 * * * cd /opt/Arb-Scanalytics && \\
               .venv/bin/python compact.py >> /var/log/arb-compact.log 2>&1
"""

import argparse
import logging
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from config import FUNDING_DIR

log = logging.getLogger("compact")


def compact_day(year: int, month: int, day: int, dry_run: bool = False) -> dict:
    """Compact one day's per-cycle files into per-venue daily files.
    Returns {venue: {status, rows?, sources, size_kb?}}."""
    partition = (
        FUNDING_DIR / f"year={year:04d}" / f"month={month:02d}" / f"day={day:02d}"
    )
    if not partition.exists():
        log.info("partition %s does not exist; nothing to do", partition)
        return {}

    # Group source files by venue. Skip already-compacted daily files.
    by_venue: dict[str, list[Path]] = {}
    for f in sorted(partition.glob("*.parquet")):
        if f.name.endswith("_daily.parquet"):
            continue
        # Filename pattern: "VENUE_<ts>.parquet" where venue may itself
        # contain a dot ("XT.COM_...", "GATE.IO_..."). rsplit on the
        # final underscore correctly separates the timestamp suffix.
        venue = f.name.rsplit("_", 1)[0]
        by_venue.setdefault(venue, []).append(f)

    if not by_venue:
        log.info("partition %s already compacted", partition)
        return {}

    stats: dict[str, dict] = {}
    for venue, sources in by_venue.items():
        target = partition / f"{venue}_daily.parquet"

        # Conflict: a daily file exists alongside source files. Either a
        # prior run died after rename but before delete, or someone added
        # files manually. Don't risk destroying data — log and skip.
        if target.exists():
            log.warning(
                "[%s] %s exists but %d source files remain; "
                "skipping (manual cleanup needed)",
                venue,
                target.name,
                len(sources),
            )
            stats[venue] = {"status": "conflict", "sources": len(sources)}
            continue

        if dry_run:
            log.info(
                "[%s] DRY RUN: would compact %d files into %s",
                venue,
                len(sources),
                target.name,
            )
            stats[venue] = {"status": "dry-run", "sources": len(sources)}
            continue

        temp = partition / f"{venue}_daily.parquet.tmp"
        try:
            tables = [pq.read_table(s) for s in sources]
            # promote_options="default" tolerates additive schema changes
            # mid-day (e.g., a new column was added). Older rows get NULL
            # for the new column.
            combined = pa.concat_tables(tables, promote_options="default")
            n_rows = combined.num_rows

            pq.write_table(combined, temp, compression="snappy")

            check_rows = pq.read_metadata(temp).num_rows
            if check_rows != n_rows:
                log.error(
                    "[%s] row-count mismatch: wrote %d, read back %d. "
                    "aborting; sources preserved.",
                    venue,
                    n_rows,
                    check_rows,
                )
                temp.unlink(missing_ok=True)
                stats[venue] = {"status": "error", "sources": len(sources)}
                continue

            # Atomic on POSIX: same-directory rename.
            temp.rename(target)

            for s in sources:
                s.unlink()

            size_kb = target.stat().st_size / 1024
            log.info(
                "[%s] %d rows from %d files -> %s (%.1f KB)",
                venue,
                n_rows,
                len(sources),
                target.name,
                size_kb,
            )
            stats[venue] = {
                "status": "ok",
                "rows": n_rows,
                "sources": len(sources),
                "size_kb": size_kb,
            }
        except Exception:
            log.exception("[%s] compaction failed; sources preserved", venue)
            temp.unlink(missing_ok=True)
            stats[venue] = {"status": "error", "sources": len(sources)}

    return stats


def main():
    p = argparse.ArgumentParser(description="Daily parquet compaction")
    p.add_argument(
        "--day",
        default=None,
        help="UTC day to compact (YYYY-MM-DD). Default: yesterday.",
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would happen; don't write/delete.",
    )
    args = p.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    if args.day:
        target = datetime.strptime(args.day, "%Y-%m-%d").date()
    else:
        target = (datetime.now(timezone.utc) - timedelta(days=1)).date()

    log.info(
        "compacting %s%s", target.isoformat(), " (DRY RUN)" if args.dry_run else ""
    )
    stats = compact_day(target.year, target.month, target.day, dry_run=args.dry_run)

    if not stats:
        return
    total_sources = sum(s.get("sources", 0) for s in stats.values())
    total_rows = sum(s.get("rows", 0) for s in stats.values())
    failed = sum(1 for s in stats.values() if s.get("status") == "error")
    log.info(
        "=== %d venues, %d files -> %d rows; failures: %d ===",
        len(stats),
        total_sources,
        total_rows,
        failed,
    )
    if failed:
        sys.exit(1)


if __name__ == "__main__":
    main()
