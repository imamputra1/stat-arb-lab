from pathlib import Path
import polars as pl

# Setup Path
PROJECT_ROOT = Path(__file__).parent.parent.parent.absolute()
SILVER_PATH = PROJECT_ROOT / "data" / "silver"

def inspect():
    print(f"🔍 INSPECTING SILVER LAKE: {SILVER_PATH}")
    
    # Scan Lazy
    lf = pl.scan_parquet(SILVER_PATH / "**/*.parquet")
    
    # 1. Cek Schema (Harus Float64 untuk fitur features)
    print("\n[1] SCHEMA CHECK:")
    schema = lf.collect_schema()
    for name, dtype in schema.items():
        if "z_score" in name or "beta" in name:
            print(f"   - {name}: {dtype}")

    # 2. Peek Data (Lihat 5 baris terakhir 2025)
    print("\n[2] DATA PREVIEW (Last 5 Rows of 2025):")
    df = lf.filter(pl.col("year") == "2025").tail(5).collect()
    print(df)

if __name__ == "__main__":
    inspect()
