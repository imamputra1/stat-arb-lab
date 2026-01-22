import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path
from datetime import datetime
from typing import List
import logging

from ..shared import Result, Ok, Err, OHLCV, FetchJob

class ParquetStorageAdaptor:

    def __init__(self, base_path: str = "./data/raw") -> None:
        self.base_path = Path(base_path)
        self.base_path.mkdir(parents=True, exist_ok=True)

    async def save(self, data: List[OHLCV], job: FetchJob) -> Result[bool, str]:

        try:
            if not data:
                return Ok(True)

            df = pd.DataFrame([item.model_dump() for item in data])
            safe_symbol = job.symbol.replace("/", "-")
            save_dir = self.base_path / safe_symbol / job.timeframe
            save_dir.mkdir(parents=True, exist_ok=True)

            first_ts = data[0].timestamp
            date_obj = datetime.fromtimestamp(first_ts / 1000)
            filename = f"{date_obj.year}-{date_obj.month:02d}.parquet"
            file_path = save_dir / filename

            new_table = pa.Table.from_pandas(df)
            
            if file_path.exists():
                try:
                    existing_table = pq.read_table(file_path)
                    
                    combined_table = pa.concat_tables([existing_table, new_table])
                    
                    combined_df = combined_table.to_pandas()
                    
                    combined_df = combined_df.drop_duplicates(subset=['timestamp'], keep='last')
                    
                    combined_df = combined_df.sort_values(by='timestamp')
                    
                    final_table = pa.Table.from_pandas(combined_df)
                except Exception as read_err:
                    logging.warning(f"Corrupt parquet file found at {file_path}, overwriting. Error: {read_err}")
                    final_table = new_table
            else:
                final_table = new_table
            pq.write_table(
                final_table, 
                file_path, 
                compression='zstd', 
                compression_level=3
            )
            
            logging.info(f"Saved {len(data)} rows to {file_path} (ZSTD)")
            return Ok(True)

        except Exception as e:
            logging.error(f"Storage Error: {e}")
            return Err(f"Parquet Write Failed: {str(e)}")
