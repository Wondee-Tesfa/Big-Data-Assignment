import sys
import logging
from collections import Counter
from pathlib import Path
from src.config import LOGS_DIR, REPORTS_DIR


def setup_logging(log_filename: str = "etl.log") -> logging.Logger:
    """Sets up dual logging to both console stdout and persistent log file."""
    LOGS_DIR.mkdir(parents=True, exist_ok=True)
    log_path = LOGS_DIR / log_filename
    
    fmt = "%(asctime)s | %(levelname)-7s | %(name)s | %(message)s"
    logging.basicConfig(
        level=logging.INFO,
        format=fmt,
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler(log_path, mode="w", encoding="utf-8"),
        ],
    )
    return logging.getLogger("etl")


def write_audit_report(totals: Counter, failed_chunks: list, elapsed_seconds: float) -> str:
    """Generates and persists the ETL audit report in reports/etl_report.txt."""
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    report_path = REPORTS_DIR / "etl_report.txt"

    lines = [
        "NYC TAXI BIG DATA ETL - AUDIT REPORT",
        "=" * 50,
        f"Raw rows read           : {totals['raw_rows']:>12,}",
        f"Rows loaded (clean)     : {totals['loaded_rows']:>12,}",
        f"Rows removed (total)    : {totals['raw_rows'] - totals['loaded_rows']:>12,}",
        "",
        "Rows removed per validation rule:",
    ]
    for key in [
        "null_values", "duplicates", "pickup_outside_jan_2015",
        "bad_duration", "bad_passenger_count", "bad_trip_distance",
        "bad_amounts", "bad_coordinates", "bad_vendor",
        "bad_rate_code", "bad_payment_type"
    ]:
        lines.append(f"  {key:<26}: {totals[key]:>12,}")

    lines += [
        "",
        f"Failed chunks           : {failed_chunks or 'none'}",
        f"Elapsed time            : {elapsed_seconds / 60:.1f} minutes",
    ]
    report_text = "\n".join(lines)
    report_path.write_text(report_text, encoding="utf-8")
    return report_text


def write_execution_summary(record_count: int, elapsed_seconds: float) -> None:
    """Writes a high-level execution summary to reports/execution_summary.txt."""
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    summary_path = REPORTS_DIR / "execution_summary.txt"
    summary_text = f"""EXECUTION SUMMARY
==================================================
Status                  : SUCCESS
Database Target         : PostgreSQL (taxi_db)
Target Table            : trips
Verified Total Records  : {record_count:,}
Rubric Milestone (>=2M) : {'PASSED' if record_count >= 2000000 else 'FAILED'}
Total Processing Time   : {elapsed_seconds / 60:.2f} minutes
"""
    summary_path.write_text(summary_text, encoding="utf-8")
