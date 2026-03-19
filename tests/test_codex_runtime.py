import os
import sys

# Add tools/ to path so we can import runtime helpers
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "tools"))


def test_build_codex_override_args_includes_command_env_and_url():
    from dcc_runtime.codex_backend import build_codex_override_args

    overrides = build_codex_override_args(
        {
            "stdio_demo": {
                "command": "/usr/bin/env",
                "args": ["python3", "-m", "demo"],
                "env": {"FOO": "bar"},
            },
            "http_demo": {
                "url": "http://127.0.0.1:8123/mcp",
            },
        }
    )

    assert 'mcp_servers.stdio_demo.command="/usr/bin/env"' in overrides
    assert 'mcp_servers.stdio_demo.args=["python3", "-m", "demo"]' in overrides
    assert 'mcp_servers.stdio_demo.env={FOO = "bar"}' in overrides
    assert 'mcp_servers.http_demo.url="http://127.0.0.1:8123/mcp"' in overrides


def test_toml_literal_supports_nested_structures():
    from dcc_runtime.codex_backend import _toml_literal

    assert _toml_literal(True) == "true"
    assert _toml_literal(["a", "b"]) == '["a", "b"]'
    assert _toml_literal({"A": "b", "ENABLED": True}) == '{A = "b", ENABLED = true}'


def test_compose_runtime_prompt_appends_claude_md_for_codex_only(tmp_path):
    from orchestrator_daemon import _compose_runtime_prompt

    (tmp_path / "CLAUDE.md").write_text("Shared repo instructions")

    claude_prompt, _ = _compose_runtime_prompt(str(tmp_path), "worker", "claude")
    codex_prompt, _ = _compose_runtime_prompt(str(tmp_path), "worker", "codex")

    assert "Shared repo instructions" not in claude_prompt
    assert "Shared repo instructions" in codex_prompt


def test_normalize_codex_defaults_follow_permission_mode():
    from orchestrator_daemon import (
        _normalize_codex_approval_policy,
        _normalize_codex_sandbox_mode,
    )

    assert _normalize_codex_sandbox_mode("", permission_mode="bypassPermissions") == "danger-full-access"
    assert _normalize_codex_sandbox_mode("", permission_mode="default") == "workspace-write"
    assert _normalize_codex_approval_policy("on-request") == "on-request"
    assert _normalize_codex_approval_policy("") == "never"
