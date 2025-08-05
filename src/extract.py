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

# Constants for GitHub API
BASE = "https://api.github.com"

# Read personal access token from environment variable
TOKEN = os.getenv("GH_TOKEN")

HEADERS = {
    "User-Agent": "github-etl",
    "Accept": "application/vnd.github+json",
    **({"Authorization": f"Bearer {TOKEN}"} if TOKEN else {}),
}

# Rate limiting constants
# The cushion time to wait after hitting the rate limit
# This is added to the reset time to ensure we don't hit the limit again immediately
RATE_CUSHION = 10

# Fields to extract from the repository data
FIELDS = (
    "id", "full_name", "private", "fork", "fork_count" , "stargazers_count",
    "watchers_count", "language", "license", "created_at",
    "updated_at", "pushed_at", "size", "default_branch", "open_issues_count",
    "topics", "visibility", "archived", "disabled", "has_issues"
)

# Configuration file path
CONFIG_PATH = Path(__file__).resolve().parent.parent / "config.toml"

def load_config() -> dict:
    """Load configuration from a TOML file."""
    if not CONFIG_PATH.exists():
        raise FileNotFoundError(f"Configuration file not found: {CONFIG_PATH}")
    
    with open(CONFIG_PATH, "rb") as f:
        config = tomllib.load(f)
    
    return config



# Function to make a GET request to the GitHub API with rate limiting
def _request(url:str, params:dict | None = None, headers:dict | None = None) -> httpx.Response:
    """Make a GET request to the GitHub API with rate limiting."""
    if headers is None:
        headers = HEADERS
    
    while True:
        response = httpx.get(url, params=params, headers=headers)
        
        if response.status_code == 403 and "X-RateLimit-Remaining" in response.headers:
            reset_time = int(response.headers["X-RateLimit-Reset"])
            wait_time = max(reset_time - int(time.time()), 0) + RATE_CUSHION
            print(f"Rate limit exceeded. Waiting for {wait_time} seconds.")
            time.sleep(wait_time)
            continue
        
        response.raise_for_status()
        return response
    

# Paginate
def _paginate(url:str, params: dict | None = None) -> Sequence[httpx.Response]: # type: ignore
    """Paginate through GitHub API responses."""
    if params is None:
        params = {}
    
    while True:
        response = _request(url, params=params)
        yield response
        
        # Check for pagination links
        if "Link" not in response.headers:
            break
        
        links = response.headers["Link"].split(", ")
        next_link = None
        
        for link in links:
            if 'rel="next"' in link:
                next_link = link.split(";")[0].strip("<>")
                break
        
        if not next_link:
            break
        
        url = next_link



def list_org_repos(org: str, per_page: int = 100) -> List[str]:
    """Return full repo names (owner/name) for a GitHub org."""
    url = f"{BASE}/orgs/{org}/repos"
    params = {"per_page": per_page, "type": "public"}
    repos: List[str] = []
    for resp in _paginate(url, params):
        repos.extend(r["full_name"] for r in resp.json())
    return repos


def get_repo(full_name: str) -> Dict[str, Any]:
    """Return selected metadata for a single repo."""
    url = f"{BASE}/repos/{full_name}"
    resp = _request(url)
    data = resp.json()
    # Thin projection of fields we want
    subset = {k: data.get(k) for k in FIELDS}
    # Flatten license to its key if present
    subset["license"] = (data["license"]["spdx_id"]
                         if data.get("license") else None)
    return subset


def fetch_all() -> List[Dict[str, Any]]:
    """Entry point called by main.py / tests."""
    cfg = load_config()
    if cfg.get("org"):
        repo_list = list_org_repos(cfg["org"], cfg.get("per_page", 100))
    else:
        repo_list = cfg["repos"]
    print(f"Fetching {len(repo_list)} repositories …")
    return [get_repo(r) for r in repo_list]

