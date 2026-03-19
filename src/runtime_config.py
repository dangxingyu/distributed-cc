from __future__ import annotations

import json
from dataclasses import dataclass


RUNTIME_FIELDS = (
    "provider",
    "model",
    "session_model",
    "permission_mode",
    "sandbox_mode",
    "approval_policy",
)


def _as_dict(value) -> dict:
    return value if isinstance(value, dict) else {}


def _norm_text(value) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _normalize_provider(value: str | None, default: str = "claude") -> str:
    candidate = _norm_text(value).lower() or default
    return candidate if candidate in {"claude", "codex"} else default


def _provider_overrides(mapping: dict, provider: str) -> dict:
    providers = _as_dict(mapping.get("providers"))
    return _as_dict(providers.get(provider))


def _fragments_for_source(source: dict | None, provider: str) -> list[dict]:
    mapping = _as_dict(source)
    if not mapping:
        return []
    runtime = _as_dict(mapping.get("runtime"))
    return [
        mapping,
        _provider_overrides(mapping, provider),
        runtime,
        _provider_overrides(runtime, provider),
    ]


def normalized_runtime_fragment(source: dict | None) -> str:
    mapping = _as_dict(source)
    if not mapping:
        return ""

    runtime = _as_dict(mapping.get("runtime"))
    payload = {
        field: _norm_text(mapping.get(field))
        for field in RUNTIME_FIELDS
        if field != "provider" or mapping.get(field) is not None
    }
    payload["providers"] = _as_dict(mapping.get("providers"))
    payload["runtime"] = {
        field: _norm_text(runtime.get(field))
        for field in RUNTIME_FIELDS
        if field != "provider" or runtime.get(field) is not None
    }
    payload["runtime"]["providers"] = _as_dict(runtime.get("providers"))
    return json.dumps(payload, sort_keys=True, ensure_ascii=False)


@dataclass(frozen=True, slots=True)
class RuntimeSettings:
    provider: str = "claude"
    model: str = ""
    session_model: str = ""
    permission_mode: str = ""
    sandbox_mode: str = ""
    approval_policy: str = ""


def resolve_runtime_settings(defaults: dict | None = None, *sources: dict | None) -> RuntimeSettings:
    provider = "claude"
    for source in (defaults, *sources):
        mapping = _as_dict(source)
        direct_provider = _norm_text(mapping.get("provider"))
        if direct_provider:
            provider = _normalize_provider(direct_provider, default=provider)
        runtime = _as_dict(mapping.get("runtime"))
        runtime_provider = _norm_text(runtime.get("provider"))
        if runtime_provider:
            provider = _normalize_provider(runtime_provider, default=provider)

    merged = {field: "" for field in RUNTIME_FIELDS}
    merged["provider"] = provider

    for source in (defaults, *sources):
        for fragment in _fragments_for_source(source, provider):
            for field in RUNTIME_FIELDS:
                value = _norm_text(fragment.get(field))
                if not value:
                    continue
                if field == "provider":
                    merged[field] = _normalize_provider(value, default=merged[field] or provider)
                else:
                    merged[field] = value

    return RuntimeSettings(
        provider=merged["provider"] or provider,
        model=merged["model"],
        session_model=merged["session_model"],
        permission_mode=merged["permission_mode"],
        sandbox_mode=merged["sandbox_mode"],
        approval_policy=merged["approval_policy"],
    )
