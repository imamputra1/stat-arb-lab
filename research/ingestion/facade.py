"""
Alasan: Ada loader, adaptor, streamer, storage. User tidak peduli data datang dari CSV, Parquet, atau API Yahoo.

    Lokasi: research/ingestion/facade.py

    Nama Class: DataIngestionFacade

    Tugas: load_data(symbol, timeframe). Facade akan memilih adaptor yang tepat dan memutuskan apakah ambil dari cache (storage) atau download baru (streamer).
"""
