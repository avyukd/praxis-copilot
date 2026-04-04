"""Shared environment loader for all praxis daemons.

Loads SNS_TOPIC_ARN, AWS_DEFAULT_REGION, and other critical env vars
from .env.deploy so background processes don't need them in launchd plists.

Call load_env() at daemon startup.
"""

from __future__ import annotations

import os
from pathlib import Path


def load_env() -> None:
    """Load environment variables from .env.deploy if not already set."""
    repo_root = Path(__file__).resolve().parent.parent.parent
    env_file = repo_root / ".env.deploy"

    if not env_file.exists():
        return

    for line in env_file.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if "=" not in line:
            continue
        key, _, value = line.partition("=")
        key = key.strip()
        value = value.strip().strip('"').strip("'")

        # Only set if not already in environment (don't override explicit env)
        if key not in os.environ or not os.environ[key]:
            os.environ[key] = value

    # Always ensure AWS region
    if "AWS_DEFAULT_REGION" not in os.environ:
        os.environ["AWS_DEFAULT_REGION"] = "us-east-1"
