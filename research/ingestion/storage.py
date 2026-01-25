import os
import asyncio
import logging
from pathlib import Path
from typing import List, Optional

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from ..shared import Result, Ok, Err, OHLCV, FetchJob

logger = logging.getLogger(__name__)

class ParquetStorageAdaptor:
    """
    Storage Adaptor dengan Hive Partitioning (Year/Month).
    Menangani penyimpanan data besar dengan efisien menggunakan Parquet ZSTD.
    """

    def __init__(self, base_path: str = "./data/raw") -> None:
        self.base_path = Path(base_path)
        try:
            self.base_path.mkdir(parents=True, exist_ok=True)
            logger.info(f"Storage Initialized at: {self.base_path.absolute()}")
        except Exception as e:
            logger.error(f"Failed to Initialize Storage at {base_path}: {e}")
            raise

    # Sekarang kita bisa menggunakan OHLCV dan FetchJob tanpa tanda kutip
    async def save(self, data: List[OHLCV], job: FetchJob) -> Result[bool, str]:
        if not data:
            logger.warning(f"No data to save for {job.symbol}")
            return Ok(True)
        try:
            df_result = await self._create_partitioned_dataframe(data)
            if df_result.is_err():
                return Err(df_result.error)
            
            df = df_result.unwrap()

            if df.empty:
                return Err("DataFrame is empty after processing")

            save_result = await self._save_monthly_partitions(df, job)
            if save_result.is_err():
                return Err(f"Failed to save partitions: {save_result.error}")

            saved_files = save_result.unwrap()

            logger.info(f"Distributed {len(data)} rows into {len(saved_files)} monthly files for job {job.symbol}")
            return Ok(True)

        except Exception as e:
            error_msg = f"Storage Error for {job.symbol}: {str(e)}"
            logger.error(error_msg, exc_info=True)
            return Err(error_msg)

    async def _create_partitioned_dataframe(self, data: List[OHLCV]) -> Result[pd.DataFrame, str]:
        """ Convert OHLCV list to DataFrame dengan result pattern"""
        try:
            dict_list = []
            for item in data:
                try:
                    dict_list.append(item.model_dump())
                except Exception:
                    continue

            if not dict_list:
                return Err("No valid OHLCV items to convert")

            df = pd.DataFrame(dict_list)

            # Tambahkan kolom partisi
            df['datetime'] = pd.to_datetime(df['timestamp'], unit='ms')
            df['month_key'] = df['datetime'].dt.to_period('M').astype(str) # Format: "2023-01"

            df = df.sort_values('timestamp').reset_index(drop=True)

            return Ok(df)
        except Exception as e:
            return Err(f"Failed to Create DataFrame: {e}")

    async def _save_monthly_partitions(self, df: pd.DataFrame, job: FetchJob) -> Result[List[str], str]:
        """Save data grouped by month"""
        try:
            grouped = df.groupby('month_key')
            saved_files = []

            for month_key, group_df in grouped:
                # Pastikan month_key adalah string
                file_result = await self._process_single_month(group_df, str(month_key), job)

                if file_result.is_err():
                    logger.warning(f"Failed to save month {month_key}: {file_result.error}")
                    continue

                saved_files.append(file_result.unwrap())

            if not saved_files:
                return Err("No files were saved successfully")
            return Ok(saved_files)

        except Exception as e:
            return Err(f"Failed to save monthly partitions: {e}")

    async def _process_single_month(
        self,
        month_df: pd.DataFrame,
        month_key: str,
        job: FetchJob
    ) -> Result[str, str]:
        try:
            required_columns = ['timestamp', 'open', 'high', 'low', 'close', 'volume']
            missing_cols = [col for col in required_columns if col not in month_df.columns]

            if missing_cols:
                return Err(f"Missing columns {missing_cols} in monthly {month_key}")

            clean_df = month_df[required_columns].copy()

            save_path = self._get_monthly_save_path(month_key, job)
            save_path.parent.mkdir(parents=True, exist_ok=True)

            if save_path.exists():
               table_result = await self._upsert_data(save_path, clean_df)
               if table_result.is_err():
                   return Err(f"Upsert failed for {month_key}: {table_result.error}")
               final_table = table_result.unwrap()
            else:
                final_table = pa.Table.from_pandas(clean_df, preserve_index=False)

            # Write ke disk (blocking I/O di thread terpisah)
            try:
                await asyncio.to_thread(
                    pq.write_table,
                    final_table,
                    save_path,
                    compression='zstd',
                    compression_level=3
                )
            except Exception as e:
                return Err(f"Failed to write parquet file {month_key}: {e}")

            return Ok(save_path.name)
        except Exception as e:
            return Err(f"Failed to process month {month_key}: {e}")

    def _get_monthly_save_path(self, month_key: str, job: FetchJob) -> Path:
        """Generate hive-style partitions path"""
        safe_symbol = job.symbol.replace("/", "-")

        try:
            if '-' not in month_key:
                raise ValueError(f"Invalid month_key format: {month_key}")

            year, month = month_key.split('-')

            if not year.isdigit() or not month.isdigit():
                raise ValueError(f"Invalid Year/Month in month_key: {month_key}")

            save_dir = (
                self.base_path /
                f"symbol={safe_symbol}" /
                f"interval={job.timeframe}" /
                f"year={year}" /
                f"month={month.zfill(2)}"
            )
            return save_dir / "data.parquet"

        except Exception as e:
            logger.error(f"Failed to generate save path for {month_key}: {e}")
            raise

    async def _upsert_data(self, existing_path: Path, new_data: pd.DataFrame) -> Result[pa.Table, str]:
        try:
            existing_table = await asyncio.to_thread(pq.read_table, existing_path)
            existing_df = existing_table.to_pandas()

            # Gabungkan data lama dan baru
            combined_df = pd.concat([existing_df, new_data])

            # Deduplikasi
            combined_df = combined_df.drop_duplicates(
                    subset=['timestamp'],
                    keep='last'
                )
            combined_df = combined_df.sort_values('timestamp').reset_index(drop=True)

            return Ok(pa.Table.from_pandas(combined_df, preserve_index=False))

        except FileNotFoundError:
            return Ok(pa.Table.from_pandas(new_data, preserve_index=False))
        except Exception as e:
            logger.warning(f"Upsert failed, overwriting with new data: {e}")
            return Ok(pa.Table.from_pandas(new_data, preserve_index=False))

    async def list_partitions(self, symbol: str, timeframe: str) -> Result[List[str], str]:
        try:
            safe_symbol = symbol.replace("/", "-")
            pattern = f"symbol={safe_symbol}/interval={timeframe}/year=*/month=*/data.parquet"

            partitions = []
            for path in self.base_path.glob(pattern):
                parts = path.parts
                # Cek struktur path untuk safety
                if len(parts) >= 4:
                    try:
                        year = parts[-3].split('=')[1]
                        month = parts[-2].split('=')[1]
                        partitions.append(f"{year}-{month}")
                    except IndexError:
                        continue

            return Ok(sorted(partitions))
            
        except Exception as e:
            return Err(f"Failed to list partitions: {e}")

    async def get_file_size(self, symbol:str, timeframe: str, month_key: str) -> Result[Optional[int], str]:
        try:
            safe_symbol = symbol.replace("/", "-")
            year, month = month_key.split('-')

            file_path = (
                self.base_path /
                f"symbol={safe_symbol}" /
                f"interval={timeframe}" /
                f"year={year}" /
                f"month={month.zfill(2)}" /
                "data.parquet"
            )

            if file_path.exists():
                return Ok(file_path.stat().st_size)
            return Ok(None)
        except Exception as e:
            return Err(f"Failed to get file size: {e}")

    async def health_check(self) -> Result[bool, str]:
        try:
            test_file = self.base_path / ".health_check"
            test_file.touch(exist_ok=True)
            test_file.unlink()

            if not os.access(self.base_path, os.W_OK | os.R_OK):
                return Err(f"Storage path {self.base_path} is not readable/writable")
            return Ok(True)

        except Exception as e:
            return Err(f"Health check failed: {e}")
