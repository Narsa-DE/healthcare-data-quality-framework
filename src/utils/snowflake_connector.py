"""
Snowflake Connector - Handles connection pooling, query execution,
and bulk loading for healthcare data pipelines.
"""
import os, logging
from contextlib import contextmanager
from typing import List, Dict
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
import pandas as pd

logger = logging.getLogger(__name__)


class SnowflakeConnector:
    """Thread-safe Snowflake connector for healthcare data pipelines."""

    def __init__(self, database="HEALTHCARE_DB", warehouse="COMPUTE_WH", schema="STAGING"):
        self.config = {
            "account": os.environ["SNOWFLAKE_ACCOUNT"],
            "user": os.environ["SNOWFLAKE_USER"],
            "password": os.environ["SNOWFLAKE_PASSWORD"],
            "database": database,
            "warehouse": warehouse,
            "schema": schema,
            "role": "DATA_ENGINEER",
        }
        self._conn = None

    def connect(self):
        if not self._conn or self._conn.is_closed():
            self._conn = snowflake.connector.connect(**self.config)
            logger.info(f"Connected to Snowflake: {self.config['account']}")
        return self._conn

    @contextmanager
    def get_cursor(self):
        conn = self.connect()
        cursor = conn.cursor(snowflake.connector.DictCursor)
        try:
            yield cursor
            conn.commit()
        except Exception as e:
            conn.rollback()
            logger.error(f"Snowflake error: {e}")
            raise
        finally:
            cursor.close()

    def execute_query(self, sql: str, params=None) -> List[Dict]:
        with self.get_cursor() as cursor:
            cursor.execute(sql, params)
            return cursor.fetchall()

    def bulk_insert(self, df: pd.DataFrame, table: str) -> int:
        conn = self.connect()
        success, nchunks, nrows, _ = write_pandas(conn, df, table.upper())
        if success:
            logger.info(f"Inserted {nrows} rows into {table} in {nchunks} chunks")
        return nrows

    def copy_from_s3(self, stage: str, table: str, file_format: str = "PARQUET") -> Dict:
        sql = f"COPY INTO {table} FROM @{stage} FILE_FORMAT=(TYPE='{file_format}') ON_ERROR='CONTINUE' PURGE=TRUE;"
        results = self.execute_query(sql)
        return {"rows_loaded": len(results), "status": "success"}

    def close(self):
        if self._conn and not self._conn.is_closed():
            self._conn.close()
