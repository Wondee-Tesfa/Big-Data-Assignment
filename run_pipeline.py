"""
NYC Yellow Taxi Big Data ETL Pipeline Entrypoint
=================================================
Orchestrates:
  1. Extract: Memory-bounded chunked streaming from data/raw/
  2. Transform: Vectorized data cleansing & feature engineering
  3. Load: High-speed PostgreSQL COPY bulk loading
  4. Optimize: Post-load indexing & ANALYZE statistics

Usage:
  python run_pipeline.py            # Process complete dataset (~12.7M rows)
  python run_pipeline.py --chunks 5 # Process 5 chunks (~2.44M clean rows)
  python run_pipeline.py --test     # Dry-run on 1 chunk (~487k clean rows)
"""

import sys
import time
import argparse
from collections import Counter

from src.config import PROJECT_ROOT, SQL_DIR
from src.database import get_connection, run_sql_file
from src.extract import resolve_csv_path, extract_chunks
from src.transform import clean_chunk
from src.load import copy_dataframe
from src.utils import setup_logging, write_audit_report, write_execution_summary


def main() -> int:
    parser = argparse.ArgumentParser(description="NYC Yellow Taxi ETL Pipeline -> PostgreSQL")
    parser.add_argument("--test", action="store_true",
                        help="load only the first chunk (shortcut for --chunks 1)")
    parser.add_argument("--chunks", type=int, default=None,
                        help="maximum number of chunks to process (e.g. 5 for ~2.44M rows; default: all)")
    parser.add_argument("--source", type=str, default=None,
                        help="optional explicit path to source CSV file")
    args = parser.parse_args()

    max_chunks = 1 if args.test else args.chunks

    log = setup_logging()
    started = time.time()
    totals: Counter = Counter()
    failed_chunks: list[int] = []

    try:
        csv_path = resolve_csv_path(args.source)
        log.info("Source dataset resolved: %s", csv_path)
        conn = get_connection()
    except Exception as exc:
        log.error("Setup or connection initialization failed: %s", exc)
        return 1

    try:
        # Step 1: Initialize Database DDL & Seed Lookups
        schema_path = SQL_DIR / "schema.sql"
        seed_path = SQL_DIR / "seed_data.sql"
        run_sql_file(conn, schema_path)
        if seed_path.is_file():
            run_sql_file(conn, seed_path)

        # Step 2: Stream, Transform, and Load Chunks
        for i, raw_chunk in enumerate(extract_chunks(csv_path), start=1):
            try:
                clean_df, stats = clean_chunk(raw_chunk)
                copy_dataframe(conn, clean_df)
                totals.update(stats)
                totals["loaded_rows"] += len(clean_df)
                log.info(
                    "Chunk %02d | read: %s | clean loaded: %s | cumulative loaded: %s",
                    i, f"{len(raw_chunk):,}", f"{len(clean_df):,}", f"{totals['loaded_rows']:,}"
                )
            except Exception as exc:
                conn.rollback()
                failed_chunks.append(i)
                log.exception("Chunk %d failed and was rolled back cleanly: %s", i, exc)

            if max_chunks and i >= max_chunks:
                log.info("Reached target limit of %d chunk(s). Stopping ingestion.", max_chunks)
                break

        # Step 3: Post-Load Optimization & Indexing
        indexes_path = SQL_DIR / "indexes.sql"
        if indexes_path.is_file():
            run_sql_file(conn, indexes_path)

        # Step 4: Final Database Record Count Verification
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM trips;")
            final_count = cur.fetchone()[0]

        log.info("Verified rows now stored in PostgreSQL 'trips' table: %s", f"{final_count:,}")

    except Exception as exc:
        log.exception("Fatal pipeline execution failure: %s", exc)
        return 1
    finally:
        conn.close()

    elapsed = time.time() - started
    audit_text = write_audit_report(totals, failed_chunks, elapsed)
    write_execution_summary(totals["loaded_rows"], elapsed)
    log.info("\n%s", audit_text)

    return 1 if failed_chunks else 0


if __name__ == "__main__":
    sys.exit(main())
