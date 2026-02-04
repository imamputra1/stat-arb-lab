"""
DOCTOR ORCA - THE QUANTUM ARCHITECT v12.2 (Final Wiring Edition)
Location: ~/orca/check_structure.py
Focus: Full AST Refactoring with updated organ mapping for Risk, Execution, & Analytics.
"""

import sys
import ast
import shutil
import importlib.util
from pathlib import Path
from datetime import datetime
from typing import List
from concurrent.futures import ThreadPoolExecutor, as_completed

# ==============================================================================
# 1. QUANTUM CONFIGURATION
# ==============================================================================

PROJECT_ROOT = Path(__file__).parent.absolute()
sys.path.insert(0, str(PROJECT_ROOT))

# PETA TRANSFORMASI LENGKAP (THE SOURCE OF TRUTH)
# Ini adalah kunci perbaikan pipeline.py
IMPORT_TRANSFORM_TABLE = {
    # --- FOUNDATION ---
    "research.shared": "core.shared",
    "research.shared.result": "core.shared.result",
    "research.shared.domain": "core.shared.domain",
    
    # --- DATA & MATH ---
    "research.strategy.data.loader": "research.ingestion.loader",
    "research.strategy.data": "research.ingestion",
    "research.strategy.models.library.kalman": "core.math.kalman",
    "research.strategy.models.base": "core.math.base",
    "core.base": "core.math.base",
    
    # --- NEW ORGANS (IMPLANTED) ---
    # Risk Management
    "research.strategy.risk": "core.risk.manager",
    
    # Execution
    "research.strategy.execution": "core.execution", # Menggunakan __init__ di core/execution
    
    # Analytics
    "research.strategy.analytics": "research.analysis.pipeline",
    
    # Signals
    "research.strategy.signals": "core.signals",
    
    # Optimization Self-Ref
    "research.strategy.optimization": "research.strategy.optimization"
}

# Direktori yang HARUS DIABAIKAN
IGNORED_DIRS = {
    ".quantum_backup", 
    "__pycache__", 
    ".git", 
    "venv", 
    ".idea", 
    ".vscode",
    "logs",
    "htmlcov"
}

class QuantumColors:
    HEADER = '\033[95m'
    BLUE = '\033[94m'
    GREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    DIM = '\033[2m'

    @staticmethod
    def log(level: str, message: str, detail: str = ""):
        timestamp = datetime.now().strftime("%H:%M:%S")
        icons = {
            "INFO": ("ℹ️", QuantumColors.BLUE),
            "SUCCESS": ("✅", QuantumColors.GREEN),
            "WARN": ("⚠️", QuantumColors.WARNING),
            "ERROR": ("❌", QuantumColors.FAIL),
            "QUANTUM": ("🔮", QuantumColors.HEADER),
            "SURGERY": ("💉", QuantumColors.HEADER)
        }
        icon, color = icons.get(level, ("?", QuantumColors.ENDC))
        print(f"{QuantumColors.DIM}[{timestamp}]{QuantumColors.ENDC} {color}{icon} {message}{QuantumColors.ENDC}")
        if detail: print(f"           {QuantumColors.DIM}└─ {detail}{QuantumColors.ENDC}")

# ==============================================================================
# 2. AST REFACTORING ENGINE
# ==============================================================================

class ImportRefactorer(ast.NodeTransformer):
    def __init__(self, file_path: Path):
        self.file_path = file_path
        self.modifications = []
        self.dirty = False

    def visit_Import(self, node):
        new_names = []
        for alias in node.names:
            if alias.name in IMPORT_TRANSFORM_TABLE:
                new_target = IMPORT_TRANSFORM_TABLE[alias.name]
                self.modifications.append(f"Rewired: import {alias.name} -> {new_target}")
                new_names.append(ast.alias(name=new_target, asname=alias.asname))
                self.dirty = True
            else:
                new_names.append(alias)
        if self.dirty: node.names = new_names
        return node

    def visit_ImportFrom(self, node):
        module = node.module or ""
        
        # 1. Check Absolute Fix
        if module in IMPORT_TRANSFORM_TABLE:
            new_module = IMPORT_TRANSFORM_TABLE[module]
            self.modifications.append(f"Re-routed: from {module} -> {new_module}")
            node.module = new_module
            self.dirty = True
            
        # 2. Check Relative Fix (from ...shared)
        elif node.level > 0:
            if module == "shared":
                self.modifications.append("Stabilized: from ...shared -> core.shared")
                node.level = 0
                node.module = "core.shared"
                self.dirty = True
            elif module == "data" and "strategy" in str(self.file_path):
                self.modifications.append("Stabilized: from ..data -> research.ingestion")
                node.level = 0
                node.module = "research.ingestion"
                self.dirty = True
                
        return node

class SourceCodeSynthesizer:
    @staticmethod
    def heal_file(file_path: Path) -> List[str]:
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                source = f.read()
            
            tree = ast.parse(source)
            refactorer = ImportRefactorer(file_path)
            new_tree = refactorer.visit(tree)
            
            if refactorer.dirty:
                ast.fix_missing_locations(new_tree)
                if sys.version_info >= (3, 9):
                    new_source = ast.unparse(new_tree)
                    with open(file_path, 'w', encoding='utf-8') as f:
                        f.write(new_source)
                    return refactorer.modifications
                else:
                    return ["Python 3.9+ required for auto-healing"]
            return []
        except Exception:
            return [] # Silent fail on syntax errors, let verification catch it

# ==============================================================================
# 3. DOCTOR ORCA CONTROLLER
# ==============================================================================

class DoctorOrca:
    def __init__(self):
        self.root = PROJECT_ROOT
        self.backup_dir = self.root / ".quantum_backup" / datetime.now().strftime("%Y%m%d_%H%M%S")
        self.stats = {"scanned": 0, "healed": 0}

    def _is_safe_path(self, path: Path) -> bool:
        """Mencegah Recursive Loop & Folder Sampah"""
        parts = path.parts
        for ignored in IGNORED_DIRS:
            if ignored in parts:
                return False
        return True

    def create_snapshot(self):
        QuantumColors.log("QUANTUM", "Creating safe snapshot...")
        try:
            self.backup_dir.mkdir(parents=True, exist_ok=True)
            files_to_backup = []
            
            for py_file in self.root.rglob("*.py"):
                if self._is_safe_path(py_file):
                    files_to_backup.append(py_file)
            
            for src in files_to_backup:
                rel_path = src.relative_to(self.root)
                dest = self.backup_dir / rel_path
                dest.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(src, dest)
                
            QuantumColors.log("SUCCESS", f"Snapshot secure: {len(files_to_backup)} files backed up.")
        except Exception as e:
            QuantumColors.log("ERROR", "Snapshot failed", str(e))
            sys.exit(1)

    def perform_surgery(self):
        QuantumColors.log("SURGERY", "Beginning AST Surgery...")
        
        target_files = [
            f for f in self.root.rglob("*.py") 
            if self._is_safe_path(f) and f.name != "check_structure.py"
        ]
        self.stats["scanned"] = len(target_files)
        
        with ThreadPoolExecutor() as executor:
            future_to_file = {
                executor.submit(SourceCodeSynthesizer.heal_file, f): f 
                for f in target_files
            }
            
            for future in as_completed(future_to_file):
                f = future_to_file[future]
                try:
                    mods = future.result()
                    if mods:
                        self.stats["healed"] += 1
                        QuantumColors.log("SUCCESS", f"Healed {f.relative_to(self.root)}")
                        for mod in mods:
                            print(f"      🔧 {mod}")
                except Exception: pass

    def verify_vitals(self):
        QuantumColors.log("QUANTUM", "Verifying Import Vitals...")
        
        # Daftar modul kritis yang wajib hidup
        critical = [
            "config.loader", 
            "core.shared.result", 
            "core.math.kalman",
            "research.ingestion.loader", 
            
            # The Big Three (Previously Broken)
            "research.strategy.pipeline",
            "research.strategy.optimization.shotgun",
            "research.strategy.engine.vectorized"
        ]
        
        alive = True
        for mod in critical:
            try:
                # Force reload untuk memastikan perubahan terbaca
                if mod in sys.modules: del sys.modules[mod]
                importlib.import_module(mod)
                print(f"   {QuantumColors.GREEN}●{QuantumColors.ENDC} Link OK: {mod}")
            except ImportError as e:
                print(f"   {QuantumColors.FAIL}○{QuantumColors.ENDC} Link Broken: {mod}")
                print(f"      └─ {e}")
                alive = False
            except Exception as e:
                print(f"   {QuantumColors.FAIL}💥{QuantumColors.ENDC} Crash: {mod}")
                print(f"      └─ {e}")
                alive = False
        return alive

    def execute(self):
        print(f"\n{QuantumColors.HEADER}🐋 DOCTOR ORCA v12.2 (Final Wiring Edition){QuantumColors.ENDC}")
        self.create_snapshot()
        self.perform_surgery()
        
        if self.verify_vitals():
            print(f"\n{QuantumColors.GREEN}✅ SYSTEM GREEN. ALL LINKS ESTABLISHED.{QuantumColors.ENDC}")
        else:
            print(f"\n{QuantumColors.FAIL}❌ SYSTEM UNSTABLE. CHECK LOGS.{QuantumColors.ENDC}")

if __name__ == "__main__":
    DoctorOrca().execute()
