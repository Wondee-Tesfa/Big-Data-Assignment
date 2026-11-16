import logging
from pathlib import Path
from src.config import DB_CONFIG

log = logging.getLogger("etl.database")

# Auto-detect PostgreSQL driver (psycopg2 or psycopg v3)
try:
    import psycopg2 as pg
    DRIVER = 2
except ImportError:  # pragma: no cover
    import psycopg as pg
    DRIVER = 3


def get_connection():
    """Create and return an active PostgreSQL connection."""
    return pg.connect(**DB_CONFIG)


def run_sql_file(conn, path: Path) -> None:
    """Execute all SQL statements in a given file within a single transaction."""
    if not path.is_file():
        raise FileNotFoundError(f"SQL file not found: {path}")
    sql = path.read_text(encoding="utf-8")
    with conn.cursor() as cur:
        cur.execute(sql)
    conn.commit()
    log.info("Successfully executed %s", path.name)
