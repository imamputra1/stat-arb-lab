"""
Alasan: Ini adalah hutan belantara ETL (Extract, Transform, Load). Di dalamnya ada alignment, features, transformation, validation, dan storage. Tanpa Facade, siapapun yang ingin memproses data harus mengimport 5 modul berbeda dan tahu urutannya (Validasi -> Align -> Transform -> Feature).

    Lokasi: research/processing/facade.py

    Nama Class: DataPipelineFacade

    Tugas: Menyediakan fungsi satu tombol: process_raw_data_to_features(). User tidak perlu tahu bahwa di dalamnya ada proses Alignment atau Market Microstructure feature generation.
"""
