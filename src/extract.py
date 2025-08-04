from __future__ import annotations # Ensure compatibility with Python 3.7+
# Standard library imports
import os, sys, time
from pathlib import Path
from typing import Any, Dict, List, Sequence
import httpx
try:
    import tomllib # For Python 3.11+
except ImportError:
    import tomli as tomllib # For Python < 3.11

# Configuration file path
CONFIG_PATH = Path(__file__).resolve().parent.parent / "config.toml"

def load_config() -> dict:
    """Load configuration from a TOML file."""
    if not CONFIG_PATH.exists():
        raise FileNotFoundError(f"Configuration file not found: {CONFIG_PATH}")
    
    with open(CONFIG_PATH, "rb") as f:
        config = tomllib.load(f)
    
    return config

BASE = "https://api.github.com"
# Token for authentication, if available
HEADERS = {
    "User-Agent": "github-etl",
    "Accept": "application/vnd.github+json",
    **({"Authorization": f"Bearer {TOKEN}"} if TOKEN else {}),
}

RATE_CUSHION = 10 
RATE_LIMIT = 5000