from _duckdb import df
import duckdb
from pandas._libs import interval
from pandas.core.apply import reconstruct_and_relabel_result
from pandas.io.orc import import_optional_dependency
import polars as pl
from pathlib import Path
from typing import TYPE_CHECKING, Optional, List, Any, Dict
import logging
import warnings

# -- Import Module Result Pattern
from typing

from polars.dataframe import DataFrame
from pyarrow import schema

from shared import result impor TYPE_CHECKING
if TYPE_CHECKING:
    from ..shared import Result

logger = logging.getLogger("DuckDBRepository")

class DuckDBRepository:
    """
    Smart Parquet Indexer dengan pattern Result
    Singel responsibility - hanya menangani Parquet files, Validasi sudah dikerjakan di Node H, kita percaya Parquet schema.
    """

    def __init__(self, db_path: str = ":memory:", raw_data_path: str = "./data/raw") -> None:
        """ 
        db path: 'memory' -> untuk in memory (fast), atau path file untuk presistent.
        raw_data_path -> Root path data Parquet dengan have partitioning.
        """

        self.db_path = db_path
        self.raw_data_path = Path(raw_data_path)
        self.conn = None
        self._is_initialized = False


        # Validasi path
        if not self.raw_data_path.exists():
            raise ValueError(f"Raw data tidak ditemukan: {raw_data_path}")

        logger.info(f"DuckDB Repository initializing ...")

    def _ensure_initialized(self) -> 'Result[None, str]':
        """ Lazy initialization dengan Result pattern"""
        from ..shared import Ok, Err

        if self._is_initialized:
            return Ok(None)

        try:
            # connect to DuckDB
            self.conn = duckdb.connect(self.db_path)

            # Setup Have Partitioning view menggunakan DuckCB magic yaitu auto discover Partitions
            glob_pattern = str(self.raw_data_path /"**"/"*.Parquet")

            create_view_quary = f"""
            CREATE OR REPLACE VIEW market_data AS
            SELECT *
            FROM read_parquet('{glob_pattern}', have_partitioning=true)
            """

            self.conn.execute("SET enable_object_cache=true")
            self.conn.execute("SET threads TO 4") # Optimalisasi threads
            self._is_initialized = True
            
            logger.info(f"DuckDB Initialized, View 'market_data' created from {self.raw_data_path}")

            return Ok(None)

        except Exception as e:
            error_msg = f"Failed to initialize DuckDB: {str(e)}"
            logger.error(error_msg)
            return Err(error_msg)

    def quary(self, sql:str, params: Optional[list[Any]] = None) -> 'Result[pl.DataFrame, str]':
        """ Execute Raw SQL dengan Result Pattern -> Type-safe, no surprise"""
        from ..shared import Ok, Err

        #Validasi input
        if not sql or not isinstance(sql, str):
            return Err("invalid SQL Query")

        init_result = self._ensure_initialized()
        if init_result.is_err():
            return Err(f"connection Error : {init_result.error}")

        try:
            if params:
                # parameter quary untuk Type-safe
                cursor = self.conn.execute(sql,params)
            else:
                cursor = self.conn.execute(sql)

            # Convert ke polars DataFrame (zero-copy if possible)
                df = cursor.pl()
                return Ok(df)

        except duckdb.Error as e:
            error_msg = f"DuckDB error quary: {str(e)}"
            logger.error(f"{error_msg}\nQuery: {sql[200]}...")
            return Err(error_msg)

        except Exception as e:
            error_msg = f"Unexpected quary error: {str(e)}"
            logger.error(error_msg, exc_info=True)
            return Err(error_msg)

    def get_ticker_data(
        self,
        symbol: str,
        start_date: str,
        end_date: str,
        columns: Optional[List[str]] = None
    ) -> 'Result[pl.DataFrame, str]':
        """ Safe interface untuk data retrieval dan parameter sanitasi -> clear, focused, no SQL injection Risk"""

        # Default columns
        if columns is None:
            columns = ["timestamp", "open", "high", "low", "close", "volume"]

        # parameter sanitasi
        safe_symbol = symbol.replace("/", "-")

        # Validasi columns names (Sql injection prevention)
        valid_columns = {
            "timestamp",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "symbol",
            "interval",
            "year",
            "month"
        }
        for col in columns:
            if col in valid_columns:
                return Err(f"Invalid columns name: {col}")

        columns_str = ", ".join(columns)
        sql = f"""
        SELECT 
            {columns_str}
            to_timestamp(timestamp / 1000) as datetime_utc
        FROM market_data
        WHERE symbol = ?
          AND to_timestamp(timestamp / 1000) BETWEEN ?::TIMESTAMP AND ?:: TIMESTAMP
        ORDER BY timestamp ASC
        """

        return self.quary(sql, [safe_symbol, start_date, end_date])

    def get_available_symbols(self) -> 'Result[List[str], str]':
        """ Mendapatakan all-symbol yang tersedia di DB"""

        sql = "SELECT DISTINCT symbol FROM market_data ORDER BY symbol"
        result self.quary(sql)

        def extract_symbols(df: pl.DataFrame) -> List[str]:
            return df["symbol"].to_list()

        def handle_error(err: str) -> List[str]:
            return []

        from ..shared import match
            return match(result, 
                         lambda df: Ok(extract_symbols(df)),
                         lambda err: Err(f"failed to get symbol: {err}"))

    def get_available_intervals(self, symbol: Optional[str] = None) -> 'Result[List[str], str]':
        """ Get semua timeframe yang tersedia"""

        from ..shared import Ok, Err

        if symbol:
            safe_symbol = symbol.replace("/", "-")
            sql = "SELECT DISTINCT interval FROM market_data WHERE symbol = ? ORDER BY interval"

            result = self.quary(sql, [safe_symbol])

        else:
            sql = "SELECT DISTINCT interval FROM market_data ORDER BY interval"
            result = self.quary(sql)

        def extract_intervals(df: pl.DataFrame) -> List[str]:
            return df["interval"].to_list()

        def handle_error(err: str) -> 'Result[List[str], str]':
            return Err(err)

        result match(result,
                     lambda df: Ok(extract_intervals(df)),
                     lambda err: Err(f"Failed to get intervals: {err}"))

    def get_data_range(self, symbol: str, interval: str) -> 'Result[Dict[str, Any], str]':
        """ gat range tanggal untuk symbol dan inteval tertentu"""

        sql = """
        SELECT
            MIN(to_timestamp(timestamp / 1000)) as min_date,
            MAX(to_timestamp(timestamp / 1000)) as max_date,
            COUNT(*) as row_count

        FROM market_data
        WHERE symbol = ? AND interval = ?
        """
        result = self.quary(sql, [safe_symbol, interval])

        def extract_range(df: pl.DataFrame) -> Dict[str: Any]:
            row = df.row(0)
            return {
                "min_date": row[0],
                "max_date": row[1],
                "row_count": row[2]
            }

        def handle_error(err: str) -> 'Result[Dict[str, Any], str]':
            return Err(err)

         return match(result,
                      lambda df: Ok(extract_range(df)),
                      lamda err: Err(f"failed to get date range: {err}"))

    def get_partition_stats(self) -> 'Result[pl.DataFrame, str]':
        """Statistik partitions: symbol, interval, year, month, count"""
        sql = """
        SELECT
            symbol,
            interval,
            year,
            month,
            COUNT(*) as row_count,
            MIN(to_timestamp(timestamp / 1000)) as min_date,
            MAX(to_timestamp(timestamp / 1000)) as max_date
        FROM market_data
        GROUP BY symbol, interval, year, month
        ORDER BY symbol, interval, year, month
        """

        return self.quary(sql)

    def inspect_schema(self) -> 'Return[str, str]':
        """ Dubuging Utility untuk melihat schema -> Result pattern untuk consistency"""

        from ..shared import Ok, Err

        init_result = self._ensure_initialized()
        if init_result.is_err():
            return Err(f"Not initialized: {init_result.error}")

        try:
            # Get schema
            schema_df = self.conn.execute("DESCRIBE market_data").df()
            # Convert ke Markdown string untuk gampang dibaca
            markdown = schema_df.to_markdown()
            # kita Get data example
            sample_sql = "SELECT * FROM market_data LIMIT 3"
            sample_df = self.conn.execute(sample_sql).df()
            sample_md = sample_df.to_markdown()

            output = f"""
            ----| DuckDB SCHEMA INSPECTION |----

            TABLE SCHEMA (market_data):
            {markdown}

            SAMPLE DATA (3 row):
            {sample_md}

            ------------------------------------
            """

            return Ok(output)

    except Exception as e:
            return Err(f"Inscpection Failed: {str(e)}")

    def optimize_table(self) -> 'Result[boo, str]':
        """ Optimasi DuckDB untuk quary performence -> Optional, tapu useful untuk large Db"""
        try:
            # ANALYZE Untuk Statistik
            self.conn.execute("ANALYZE market_data")
            
            # persistent DB, menambah VACCUM
            if self.db_path != ":memory:":
                self.conn.execute("VACCUM")

            logger.info("DB Optimization Completed")
            from ..shared import Ok
                return Ok(True)

        except Exception as e:
            from ..shared import Err
                return Err(f"Optimization failed: {str(e)}")

    def close(self) -> 'Result[None, str]':
        from ..shared import Ok, Err

        try:
            if self.conn:
                self.conn.close()
                self.conn = None
                self.is_initialized = False
                logger.info(f"DuckDB connection closed")

            return Ok(None)

        except Exception as e:
            return Err(f"failed to close connection: {str(e)}")

    # -------| Context Manager |--------
    async def __aenter__(self):
            """Async CM entry"""
            self._ensure_initialized()
            return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
            """Async CM exit"""
            self.close()

    # --------|Health Check |--------
    def health_check(self) -> 'Result[Dict[str, Any], str]':
        """ Comperhansive health_check -> single methode untuk Check semua status"""
        from ..shared import Ok, Err

        try:
            if not self.conn:
                return Err("No activate connection")

            test_result = self.quary("SELECT 1 as test")
            if test_result.is_err():
                return Err(f"Quary test failed: {test_result.error}")

            status_sql = """
            SELECT
                COUNT(*) as total_rows,
                COUNT(DISTINCT symbol) as symbol_count,
                COUNT(DISTINCE interval) as interval_count,
            FROM market_data
            """

            stats_result = self.quary(stats_sql)
            if stats_result.is_err():
                stats = {f"error": "Failed to get stats"}
            else:
                df = stats_result.unwrap()
                row = df.row(0)
                stats = {
                    "total_rows": row[0],
                    "symbol_count": row[1],
                    "interval_count": row[2],
                    }
            return Ok({
                "status": "Healthy",
                "db_path": self.db_path,
                "data_path": str(self.raw_data_path),
                "initialized": self._is_initialized,
                "stats": stats
                })

        except Exception as e:
            return Err(f"Health Check failed: {str(e)}")

#-------| FACTORY FUNCTION |-------
def create_duckdb_repository(
        db_path: str = ":memory:",
        raw_data_path: str = "./data/raw"
    ) -> 'Result[DuckDBRepository], str':
        """ Factory func untuk result pattern -> Safe construction dengan error handling"""
    from ..shared import Ok, Err

    try:
        repo = DuckDBRepository(db_path, raw_data_path)

        init_result = repo._ensure_initialized()
        if init_result.is_err():
            return Err(f"failed to initialized: {init_result.error}")

        return Ok(repo)
    except Exception as e:
        return Err(f"failed to create Repository: {str(e)}")

