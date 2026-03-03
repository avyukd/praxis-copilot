"""Config file utilities for the Praxis CLI."""

from pathlib import Path
from typing import Any

import yaml


def find_repo_root() -> Path:
    """Find the repo root by looking for pyproject.toml or .git."""
    current = Path.cwd()
    for parent in [current, *current.parents]:
        if (parent / ".git").exists() or (parent / "pyproject.toml").exists():
            return parent
    raise SystemExit("Not inside a praxis repository. Could not find .git or pyproject.toml.")


def get_config_dir() -> Path:
    """Get the config directory path."""
    config_dir = find_repo_root() / "config"
    if not config_dir.exists():
        config_dir.mkdir(parents=True)
    return config_dir


def load_yaml(path: Path) -> Any:
    """Load a YAML file, returning empty dict if file doesn't exist."""
    if not path.exists():
        return {}
    with open(path) as f:
        return yaml.safe_load(f) or {}


def save_yaml(path: Path, data: Any) -> None:
    """Save data to a YAML file."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as f:
        yaml.dump(data, f, default_flow_style=False, sort_keys=False)
