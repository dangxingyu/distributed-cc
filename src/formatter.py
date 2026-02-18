"""Minimal formatters for the router architecture.

Most formatting is now done by the web UI directly. This module
provides a few utility functions for server-side message formatting.
"""

import re


def escape_markdown_v2(text: str) -> str:
    """Escape special characters for Telegram MarkdownV2."""
    special = r"_*[]()~`>#+-=|{}.!"
    return re.sub(f"([{re.escape(special)}])", r"\\\1", text)
