import duckdb
import polars as pl
from pathlib import Path
from typing import Optional, List, Any, Dict
from core.shared import Result, Ok, Err
import logging
logger = logging.getLogger('DuckDBRepository')

class DuckDBRepository:
    """
    The City Archive (DuckDB Engine).
    Smart Parquet Indexer dengan Result Pattern.
    Single Responsibility: Menangani indexing Parquet dan menyediakan interface SQL Type-Safe.
    """

    def __init__(self, db_path: str=':memory:', raw_data_path: str='./data/raw') -> None:
        """ 
        db_path: ':memory:' (In-Memory, Cepat) atau path file .db (Persisten).
        raw_data_path: Root path data Parquet dengan Hive Partitioning.
        """
        self.db_path = db_path
        self.raw_data_path = Path(raw_data_path)
        self.conn = None
        self._is_initialized = False
        if not self.raw_data_path.exists():
            logger.warning(f'Raw data path not found during init: {raw_data_path}')
        logger.info(f'DuckDB Repository configured at {db_path}')

    def _ensure_initialized(self) -> Result[None, str]:
        """Lazy initialization dengan Result pattern"""
        if self._is_initialized:
            return Ok(None)
        try:
            self.conn = duckdb.connect(self.db_path)
            glob_pattern = str(self.raw_data_path / '**' / '*.parquet')
            create_view_query = f"\n            CREATE OR REPLACE VIEW market_data AS\n            SELECT *\n            FROM read_parquet('{glob_pattern}', hive_partitioning=true)\n            "
            self.conn.execute(create_view_query)
            self.conn.execute('SET enable_object_cache=true')
            self._is_initialized = True
            logger.info(f"DuckDB Initialized. View 'market_data' indexed from {self.raw_data_path}")
            return Ok(None)
        except Exception as e:
            error_msg = f'Failed to initialize DuckDB: {str(e)}'
            logger.error(error_msg)
            return Err(error_msg)

    def query(self, sql: str, params: Optional[List[Any]]=None) -> Result[pl.DataFrame, str]:
        """Execute Raw SQL dengan Result Pattern -> Type-safe, Zero-Copy to Polars"""
        if not sql or not isinstance(sql, str):
            return Err('Invalid SQL Query')
        init_result = self._ensure_initialized()
        if init_result.is_err():
            return Err(f'Connection Error: {init_result.error}')
        try:
            if params:
                cursor = self.conn.execute(sql, params)
            else:
                cursor = self.conn.execute(sql)
            df = cursor.pl()
            return Ok(df)
        except duckdb.Error as e:
            error_msg = f'DuckDB SQL Error: {str(e)}'
            logger.error(f'{error_msg} | SQL: {sql[:200]}...')
            return Err(error_msg)
        except Exception as e:
            error_msg = f'Unexpected Query Error: {str(e)}'
            logger.error(error_msg, exc_info=True)
            return Err(error_msg)

    def get_ticker_data(self, symbol: str, start_date: str, end_date: str, columns: Optional[List[str]]=None) -> Result[pl.DataFrame, str]:
        """Safe interface untuk data retrieval -> No SQL Injection Risk"""
        if columns is None:
            columns = ['timestamp', 'open', 'high', 'low', 'close', 'volume']
        safe_symbol = symbol.replace('/', '-')
        valid_columns = {'timestamp', 'open', 'high', 'low', 'close', 'volume', 'symbol', 'interval', 'year', 'month'}
        for col in columns:
            if col not in valid_columns:
                return Err(f'Invalid column name request: {col}')
        columns_str = ', '.join(columns)
        sql = f'\n        SELECT \n            {columns_str},\n            to_timestamp(timestamp / 1000) as datetime_utc\n        FROM market_data\n        WHERE symbol = ?\n          AND to_timestamp(timestamp / 1000) BETWEEN CAST(? AS TIMESTAMP) AND CAST(? AS TIMESTAMP)\n        ORDER BY timestamp ASC\n        '
        return self.query(sql, [safe_symbol, start_date, end_date])

    def get_available_symbols(self) -> Result[List[str], str]:
        """Mendapatkan unique symbols yang tersedia di DB"""
        sql = 'SELECT DISTINCT symbol FROM market_data ORDER BY symbol'
        result = self.query(sql)
        if result.is_ok():
            df = result.unwrap()
            symbols = df['symbol'].to_list()
            return Ok(symbols)
        else:
            return Err(f'Failed to fetch symbols: {result.error}')

    def get_available_intervals(self, symbol: Optional[str]=None) -> Result[List[str], str]:
        """Get semua timeframe yang tersedia"""
        if symbol:
            safe_symbol = symbol.replace('/', '-')
            sql = 'SELECT DISTINCT interval FROM market_data WHERE symbol = ? ORDER BY interval'
            result = self.query(sql, [safe_symbol])
        else:
            sql = 'SELECT DISTINCT interval FROM market_data ORDER BY interval'
            result = self.query(sql)
        if result.is_ok():
            intervals = result.unwrap()['interval'].to_list()
            return Ok(intervals)
        else:
            return Err(f'Failed to fetch intervals: {result.error}')

    def get_data_range(self, symbol: str, interval: str) -> Result[Dict[str, Any], str]:
        """Get range tanggal (min, max) dan row count"""
        safe_symbol = symbol.replace('/', '-')
        sql = '\n        SELECT\n            MIN(to_timestamp(timestamp / 1000)) as min_date,\n            MAX(to_timestamp(timestamp / 1000)) as max_date,\n            COUNT(*) as row_count\n        FROM market_data\n        WHERE symbol = ? AND interval = ?\n        '
        result = self.query(sql, [safe_symbol, interval])
        if result.is_ok():
            df = result.unwrap()
            if df.height == 0:
                return Err(f'No data found for {symbol} {interval}')
            row = df.row(0)
            return Ok({'min_date': row[0], 'max_date': row[1], 'row_count': row[2]})
        else:
            return Err(f'Failed to get date range: {result.error}')

    def get_partition_stats(self) -> Result[pl.DataFrame, str]:
        """Statistik partitions: menghitung distribusi data per bulan"""
        sql = '\n        SELECT\n            symbol,\n            interval,\n            year,\n            month,\n            COUNT(*) as row_count\n        FROM market_data\n        GROUP BY symbol, interval, year, month\n        ORDER BY symbol, interval, year, month\n        '
        return self.query(sql)

    def inspect_schema(self) -> Result[str, str]:
        """Debugging Utility untuk melihat schema internal"""
        init_result = self._ensure_initialized()
        if init_result.is_err():
            return Err(f'Not initialized: {init_result.error}')
        try:
            schema_df = self.conn.execute('DESCRIBE market_data').df()
            schema_md = schema_df.to_markdown()
            sample_df = self.conn.execute('SELECT * FROM market_data LIMIT 3').df()
            sample_md = sample_df.to_markdown()
            output = f'\n----| DUCKDB SCHEMA INSPECTION |----\nTABLE SCHEMA (market_data):\n{schema_md}\n\nSAMPLE DATA (3 rows):\n{sample_md}\n------------------------------------\n            '
            return Ok(output)
        except Exception as e:
            return Err(f'Inspection Failed: {str(e)}')

    def optimize_table(self) -> Result[bool, str]:
        """Optimasi DuckDB untuk query performance (VACUUM/ANALYZE)"""
        try:
            self._ensure_initialized()
            logger.info('Starting DB Optimization (ANALYZE)...')
            self.conn.execute('ANALYZE market_data')
            if self.db_path != ':memory:':
                logger.info('VACUUMing Database...')
                self.conn.execute('VACUUM')
            logger.info('✨ DB Optimization Completed')
            return Ok(True)
        except Exception as e:
            return Err(f'Optimization failed: {str(e)}')

    def close(self) -> Result[None, str]:
        try:
            if self.conn:
                self.conn.close()
                self.conn = None
                self._is_initialized = False
                logger.info('DuckDB connection closed')
            return Ok(None)
        except Exception as e:
            return Err(f'Failed to close connection: {str(e)}')

    def __enter__(self):
        self._ensure_initialized()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def health_check(self) -> Result[Dict[str, Any], str]:
        """Comprehensive Health Check"""
        init_res = self._ensure_initialized()
        if init_res.is_err():
            return Err(f'Health Check - Init Failed: {init_res.error}')
        try:
            test_res = self.query('SELECT 1 as test')
            if test_res.is_err():
                return Err(f'Health Check - Query Test Failed: {test_res.error}')
            stats_sql = '\n            SELECT\n                COUNT(*) as total_rows,\n                COUNT(DISTINCT symbol) as symbol_count,\n                COUNT(DISTINCT interval) as interval_count\n            FROM market_data\n            '
            stats_res = self.query(stats_sql)
            if stats_res.is_ok():
                df = stats_res.unwrap()
                row = df.row(0)
                stats = {'total_rows': row[0], 'symbol_count': row[1], 'interval_count': row[2]}
            else:
                stats = {'error': 'Failed to retrieve stats'}
            return Ok({'status': 'Healthy', 'db_type': 'Memory' if self.db_path == ':memory:' else 'File', 'data_source': str(self.raw_data_path), 'stats': stats})
        except Exception as e:
            return Err(f'Health Check Exception: {str(e)}')

def create_duckdb_repository(db_path: str=':memory:', raw_data_path: str='./data/raw') -> Result[DuckDBRepository, str]:
    """Safe Factory Constructor"""
    try:
        repo = DuckDBRepository(db_path, raw_data_path)
        init_result = repo._ensure_initialized()
        if init_result.is_err():
            return Err(init_result.error)
        return Ok(repo)
    except Exception as e:
        return Err(f'Factory failed: {str(e)}')