import os
import site
import glob

def fix_redis_client():
    # 1. Cari lokasi library Redis di dalam environment
    site_packages = site.getsitepackages()
    target_file = None
    
    print("🔍 Mencari file redis/asyncio/client.py...")
    
    for path in site_packages:
        # Cari pola path yang mungkin
        potential_path = os.path.join(path, "redis", "asyncio", "client.py")
        if os.path.exists(potential_path):
            target_file = potential_path
            break
            
    if not target_file:
        # Coba fallback pencarian manual jika site packages tidak ketemu
        # (Terutama untuk uv venv)
        venv_path = os.environ.get("VIRTUAL_ENV")
        if venv_path:
             search_path = os.path.join(venv_path, "lib", "python*", "site-packages", "redis", "asyncio", "client.py")
             matches = glob.glob(search_path)
             if matches:
                 target_file = matches[0]

    if not target_file or not os.path.exists(target_file):
        print("❌ Gagal menemukan file redis/asyncio/client.py")
        print("   Pastikan environment aktif dan redis terinstall.")
        return

    print(f"✅ File ditemukan: {target_file}")

    # 2. Baca file
    with open(target_file, "r", encoding="utf-8") as f:
        lines = f.readlines()

    # 3. Cari dan Hapus Type Hint yang bermasalah
    # Target: async def listen(self) -> AsyncIterator[MonitorCommandInfo]:
    # Ubah jadi: async def listen(self):
    
    new_lines = []
    fixed = False
    
    for line in lines:
        if "async def listen(self) -> AsyncIterator[MonitorCommandInfo]:" in line:
            print("🔧 Memperbaiki baris bermasalah (Type Hint AsyncIterator)...")
            # Hapus return type hint-nya
            new_line = line.replace(" -> AsyncIterator[MonitorCommandInfo]", "")
            new_lines.append(new_line)
            fixed = True
        else:
            new_lines.append(line)

    if fixed:
        # 4. Simpan perubahan
        with open(target_file, "w", encoding="utf-8") as f:
            f.writelines(new_lines)
        print("🎉 SUKSES! File Redis telah dipatch. Error seharusnya hilang.")
    else:
        print("⚠️ Tidak menemukan baris yang perlu diperbaiki. Mungkin versi Redis berbeda atau sudah diperbaiki.")

if __name__ == "__main__":
    fix_redis_client()
