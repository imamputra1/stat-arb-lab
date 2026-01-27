import logging
import sys
from pathlib import Path
from datetime import datetime

from research.repository import DuckDBRepository

def setup_logging():
    log_dir = Path("logs")
    log_dir.mkdir(exist_ok=True)

    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    log_filename = log_dir / f"inspection_{timestamp}.log"

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(levelname)s] %(message)s',
        datefmt='%H%M%S',
        handlers=[
            logging.FileHandler(log_filename, encoding='utf-8'),
            logging.StreamHandler(sys.stdout)
        ]
    )
    logging.info(f"Inspection started. log file {log_filename}")

def main():
    setup_logging()
    logger = logging.getLogger("Inspection")

    logger.info("INFRASTRUCTURE TEST: DuckDBRepository")

    abs_data_path = Path.cwd() / "data" / "raw"
    logger.info(f"Target storage path: {abs_data_path}")

    if not abs_data_path.exists():
        logger.critical(f"PATH ERROR: {abs_data_path} tidak ditemukan")
        logger.critical("pastikan script yang dijalankan dari root folder 'abr-lab'")
        return

    try:
        with DuckDBRepository(db_path=":memory:", raw_data_path=str(abs_data_path)) as repo:
            logging.info("\n[1] Performing Health Check ...")
            health = repo.health_check()

            if health.is_ok():
                stats = health.unwrap()
                logger.info("System Healthy")
                logger.info(f"DB type: {stats.get('db_type')}")
                logger.info(f"Total rows: {stats['stats'].get('total_rows', 0):,}")
                logger.info(f"Symbol: {stats['stats'].get('symbol_count')}")
            else:
                logger.error(f"Health Check Failed: {health.error}")
                return 

            logger.info("\n[2] Inspecting schema (Hive Portitioning) ...")
            schema_res = repo.inspect_schema()
            if schema_res.is_ok():
                print(schema_res.unwrap())

            target_symbol = "BTC/USDT"
            logger.info(f"\n[3] Fetching sample data for {target_symbol} ...")

            range_res = repo.get_data_range(target_symbol, "1m")
            if range_res.is_ok():
                rng = range_res.unwrap()
                logger.info(f"    Available Range: {rng['min_date']} to {rng['max_date']}")
                
            data_res = repo.get_ticker_data(
                symbol=target_symbol,
                start_date="2024-01-01 00:00:00",
                end_date="2024-01-01 01:00:00"
            )

            if data_res.is_ok():
                df = data_res.unwrap()
                logger.info(f"Retrieved {len(df)} rows via polars zero-copy")
                print(df.head())

            else:
                logger.error(f"Fetch failed: {data_res.error}")
    except Exception as e:
        logger.critical(f"Inspection crashed {e}", exc_info=True)


if __name__ == "__main__":
    main()
