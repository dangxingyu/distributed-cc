from src.runtime_config import normalized_runtime_fragment, resolve_runtime_settings


def test_resolve_runtime_settings_merges_nested_provider_overrides():
    settings = resolve_runtime_settings(
        {
            "provider": "claude",
            "model": "claude-opus-4-6",
            "session_model": "claude-sonnet-4-6",
            "permission_mode": "acceptEdits",
            "runtime": {
                "providers": {
                    "codex": {
                        "sandbox_mode": "workspace-write",
                        "approval_policy": "never",
                    }
                }
            },
        },
        {
            "runtime": {
                "provider": "codex",
                "model": "gpt-5.4",
                "providers": {
                    "codex": {
                        "session_model": "gpt-5.4",
                        "sandbox_mode": "danger-full-access",
                    }
                },
            }
        },
    )

    assert settings.provider == "codex"
    assert settings.model == "gpt-5.4"
    assert settings.session_model == "gpt-5.4"
    assert settings.permission_mode == "acceptEdits"
    assert settings.sandbox_mode == "danger-full-access"
    assert settings.approval_policy == "never"


def test_normalized_runtime_fragment_captures_nested_provider_blocks():
    fragment = normalized_runtime_fragment(
        {
            "provider": "codex",
            "model": "gpt-5.4",
            "providers": {
                "claude": {"model": "claude-sonnet-4-6"},
            },
            "runtime": {
                "approval_policy": "never",
                "providers": {
                    "codex": {"sandbox_mode": "workspace-write"},
                },
            },
        }
    )

    assert '"provider": "codex"' in fragment
    assert '"approval_policy": "never"' in fragment
    assert '"sandbox_mode": "workspace-write"' in fragment
