import logging
from io import StringIO
import pandas as pd
from src.config import DT_FORMAT, TRIP_COLUMNS
from src.database import DRIVER

log = logging.getLogger("etl.load")


def copy_dataframe(conn, df: pd.DataFrame) -> None:
    """
    Streams a cleaned DataFrame directly into PostgreSQL's trips table
    using the high-throughput binary/text COPY protocol via an in-memory buffer.
    """
    if df.empty:
        log.warning("Empty DataFrame passed to copy_dataframe; skipping load.")
        return

    # ወደ PostgreSQL ዴታቤዝ በ COPY ትዕዛዝ በቀጥታ መጫን (ፋይል ሳይከማች)
    buf = StringIO()
    df[TRIP_COLUMNS].to_csv(buf, index=False, header=False, date_format=DT_FORMAT)
    sql = f"COPY trips ({', '.join(TRIP_COLUMNS)}) FROM STDIN WITH (FORMAT csv)"

    with conn.cursor() as cur:
        if DRIVER == 2:  # psycopg2
            buf.seek(0)
            cur.copy_expert(sql, buf)
        else:  # psycopg v3
            with cur.copy(sql) as cp:
                cp.write(buf.getvalue())
    conn.commit()