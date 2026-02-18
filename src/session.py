"""Server configuration data model.

Kept minimal — the Router handles HTTP communication directly.
This module just holds the ServerConfig dataclass for config parsing.
"""

from dataclasses import dataclass


@dataclass
class ServerConfig:
    """Configuration for a remote server (used in config.json parsing)."""
    name: str
    host: str | None          # SSH destination (null = local)
    broker_port: int = 8200   # Local port forwarded to remote daemon
    ssh_options: str = ""
    work_dir: str = ""        # optional server-level fallback
