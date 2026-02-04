"""
SPACES REPAIR KIT (LEGACY BRIDGE RESTORATION)
Location: ~/orca/fix_spaces.py
Focus: Appends missing Legacy Adapter to spaces.py to fix ImportError.
"""
from pathlib import Path

# Lokasi Target
TARGET_FILE = Path("research/strategy/optimization/spaces.py")

# Kode yang Hilang (Legacy Adapter)
LEGACY_PATCH = """

# --- LEGACY COMPATIBILITY ADAPTER (The Bridge) ---
class ParameterSpace:
    \"\"\"
    Compatibility layer for legacy modules expecting 'ParameterSpace'.
    Redirects calls to the new QuantumParameterSpace engine.
    \"\"\"
    @staticmethod
    def surgical_grid():
        for res in QuantumParameterSpace.surgical_grid().generate():
            if res.is_ok(): yield res.unwrap().params

    @staticmethod
    def dirty_shotgun():
        for res in QuantumParameterSpace.dirty_shotgun().generate():
            if res.is_ok(): yield res.unwrap().params
"""

def apply_patch():
    print(f"🔧 Analyzing {TARGET_FILE}...")
    
    if not TARGET_FILE.exists():
        print("❌ Target file not found!")
        return

    content = TARGET_FILE.read_text(encoding="utf-8")
    
    # Cek apakah sudah ada untuk mencegah duplikasi
    if "class ParameterSpace:" in content:
        print("✅ Legacy Adapter already exists. No action needed.")
        return

    # Append Patch
    print("💉 Injecting Legacy Adapter...")
    with open(TARGET_FILE, "a", encoding="utf-8") as f:
        f.write(LEGACY_PATCH)
    
    print("✨ Surgery Successful: 'ParameterSpace' restored.")

if __name__ == "__main__":
    apply_patch()
