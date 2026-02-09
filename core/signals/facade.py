"""
Alasan: Anda punya factory.py, filters.py, registry, dan folder strategies/. Untuk menghasilkan sinyal, Strategy harus dibuat lewat Factory, lalu outputnya mungkin perlu disaring oleh Filter.

    Lokasi: core/signals/facade.py

    Nama Class: SignalGeneratorFacade

    Tugas: generate_signals(data). Facade ini yang akan memanggil Factory untuk spawn strategy, memberi makan data, lalu memfilter output-nya sebelum dikembalikan ke user.
"""
