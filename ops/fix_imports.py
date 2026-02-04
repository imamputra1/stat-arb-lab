import os

# Peta Penggantian (Old -> New)
REPLACEMENTS = {
    "from research.ingestion.loader": "from research.ingestion.loader",
    "from core.math.kalman": "from core.math.kalman",
    "from core.signals": "from core.signals",
    "from core.shared": "from core.shared",
    # Fix untuk war_room.py yang mengimport OptimizationClerk
    "from research.strategy.optimization.storage import OptimizationClerk": "from research.strategy.optimization.storage import OptimizationClerk",
}

def rewire_project():
    print("🔌 STARTING MASS REFACTORING...")
    count = 0
    
    # Walk through all directories
    for root, dirs, files in os.walk("."):
        if "venv" in root or ".git" in root: continue # Skip junk
        
        for file in files:
            if file.endswith(".py"):
                path = os.path.join(root, file)
                with open(path, "r", encoding="utf-8") as f:
                    content = f.read()
                
                new_content = content
                modified = False
                
                for old, new in REPLACEMENTS.items():
                    if old in new_content:
                        new_content = new_content.replace(old, new)
                        print(f"   🔧 Fixed in {path}: {old} -> {new}")
                        modified = True
                
                if modified:
                    with open(path, "w", encoding="utf-8") as f:
                        f.write(new_content)
                    count += 1

    print(f"✅ REFACTORING COMPLETE. {count} files updated.")

if __name__ == "__main__":
    rewire_project()
