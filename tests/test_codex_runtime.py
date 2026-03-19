import os
import sys

import pytest

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


def test_compose_runtime_prompt_hash_tracks_codex_shared_instructions(tmp_path):
    from orchestrator_daemon import _compose_runtime_prompt, _runtime_base_instructions

    (tmp_path / "CLAUDE.md").write_text("Shared repo instructions")

    claude_prompt, claude_hash = _compose_runtime_prompt(str(tmp_path), "worker", "claude")
    codex_prompt, codex_hash = _compose_runtime_prompt(str(tmp_path), "worker", "codex")

    assert claude_prompt == codex_prompt
    assert claude_hash != codex_hash
    assert "Shared repo instructions" in _runtime_base_instructions(str(tmp_path), "codex")
    assert _runtime_base_instructions(str(tmp_path), "claude") == ""


def test_normalize_codex_defaults_follow_permission_mode():
    from orchestrator_daemon import (
        _normalize_codex_approval_policy,
        _normalize_codex_sandbox_mode,
    )

    assert _normalize_codex_sandbox_mode("", permission_mode="bypassPermissions") == "danger-full-access"
    assert _normalize_codex_sandbox_mode("", permission_mode="default") == "workspace-write"
    assert _normalize_codex_approval_policy("on-request") == "on-request"
    assert _normalize_codex_approval_policy("") == "never"


def test_prepare_codex_home_writes_config_and_seeds_auth(tmp_path, monkeypatch):
    from dcc_runtime.base import RuntimeRequest
    from dcc_runtime.codex_backend import prepare_codex_home

    base_home = tmp_path / "base-home"
    base_home.mkdir()
    (base_home / "auth.json").write_text('{"token":"demo"}')
    monkeypatch.setenv("CODEX_HOME", str(base_home))

    runtime_home = tmp_path / "runtime-home"
    request = RuntimeRequest(
        prompt="hello",
        project_dir="/tmp/project",
        source="orchestrator",
        system_prompt="developer prompt",
        base_instructions="shared instructions",
        model="gpt-5.4",
        sandbox_mode="workspace-write",
        approval_policy="never",
        runtime_home_dir=str(runtime_home),
    )

    home_dir = prepare_codex_home(
        request,
        {"daemon": {"url": "http://127.0.0.1:8123/mcp"}},
    )

    assert home_dir == runtime_home
    assert (runtime_home / "config.toml").exists()
    config_text = (runtime_home / "config.toml").read_text()
    assert 'approval_policy = "never"' in config_text
    assert "[mcp_servers.daemon]" in config_text
    assert (runtime_home / "instructions" / "developer.md").read_text() == "developer prompt"
    assert (runtime_home / "instructions" / "base.md").read_text() == "shared instructions"

    auth_path = runtime_home / "auth.json"
    assert auth_path.exists()
    if auth_path.is_symlink():
        assert auth_path.resolve() == (base_home / "auth.json")
    else:
        assert auth_path.read_text() == '{"token":"demo"}'


@pytest.mark.asyncio
async def test_runtime_factory_dispatches_codex(monkeypatch, tmp_path):
    from dcc_runtime.base import RuntimeEvent, RuntimeRequest, RuntimeResult
    import dcc_runtime.factory as runtime_factory

    called = {}
    seen_events = []

    async def fake_run_turn(request, on_event):
        called["provider"] = "codex"
        called["request"] = request
        await on_event(RuntimeEvent(type="log_update", data="[codex] ok"))
        return RuntimeResult(session_id="thread-123", final_text="done", saw_result=True)

    async def on_event(event):
        seen_events.append((event.type, event.data))
        return None

    monkeypatch.setattr(runtime_factory, "run_codex_turn", fake_run_turn)

    result = await runtime_factory.run_turn(
        "codex",
        RuntimeRequest(prompt="hello", project_dir=str(tmp_path), source="worker"),
        on_event,
    )

    assert called["provider"] == "codex"
    assert called["request"].source == "worker"
    assert seen_events == [("log_update", "[codex] ok")]
    assert result.session_id == "thread-123"
