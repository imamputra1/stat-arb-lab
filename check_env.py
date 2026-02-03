"""
INDUSTRIAL ENVIRONMENT CHECKER (THE GATEKEEPER) - V1.0
Location: Root (~/arb-lab/check_env.py)
Focus: Dependency validation & Hardware telemetry for Ryzen 5 5600U.
"""
import sys
import os
import platform
import multiprocessing
from importlib import metadata

def check_dependencies():
    # Daftar amunisi dari requirements.txt
    required = [
        "polars", "numpy", "pandas", "numba", "joblib", 
        "scipy", "pyarrow", "zstandard", "matplotlib"
    ]
    
    print("\n" + "="*50)
    print("🚀 ORCA INDUSTRIAL PRE-FLIGHT CHECK".center(50))
    print("="*50)
    
    # 1. Hardware Stats
    cpu_count = multiprocessing.cpu_count()
    print(f"💻 SYSTEM: {platform.system()} | CPU: {cpu_count} Cores (Ryzen 5 Target)")
    
    # 2. Environment Verification
    conda_env = os.environ.get('CONDA_DEFAULT_ENV', 'Unknown')
    if conda_env != 'quant_lab':
        print(f"⚠️  WARNING: You are in ({conda_env}) env, not (quant_lab)!")
    else:
        print(f"✅ ENVIRONMENT: {conda_env}")
        
    print("-" * 50)
    
    # 3. Library Validation
    missing = []
    for lib in required:
        try:
            ver = metadata.version(lib)
            print(f"✅ {lib:18} | Version: {ver}")
        except metadata.PackageNotFoundError:
            print(f"❌ {lib:18} | NOT FOUND")
            missing.append(lib)
            
    print("-" * 50)
    
    if not missing:
        print("🟢 STATUS: INFRASTRUCTURE READY. GASPOL!")
        return True
    else:
        print(f"🔴 STATUS: {len(missing)} LIBRARIES MISSING.")
        print(f"👉 Run: pip install {' '.join(missing)}")
        return False

if __name__ == "__main__":
    success = check_dependencies()
    sys.exit(0 if success else 1)
