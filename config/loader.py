"""
CONFIGURATION LOADER
Location: config/loader.py
"""
import yaml
from pathlib import Path
from core.shared import Result, Ok, Err

class ConfigLoader:
    _instance = None
    _config = None

    @classmethod
    def load(cls, config_path: str = "config.yaml") -> Result[dict, str]:
        if cls._config: return Ok(cls._config)
        
        path = Path(config_path)
        if not path.exists(): return Err(f"Config file not found: {path}")
        
        try:
            with open(path, 'r') as f:
                cls._config = yaml.safe_load(f)
            return Ok(cls._config)
        except Exception as e:
            return Err(f"YAML Parse Error: {str(e)}")

    @classmethod
    def get(cls, key_path: str, default=None):
        """Ambil value nested: get('paths.silver_lake')"""
        if not cls._config: cls.load()
        keys = key_path.split('.')
        val = cls._config
        for k in keys:
            val = val.get(k)
            if val is None: return default
        return val
