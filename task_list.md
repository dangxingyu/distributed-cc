# Structured Task List

## P0: Runtime Autonomy
- [x] Add daemon heartbeat to detect inactivity and nudge orchestrator to keep working.
- [x] Heartbeat includes GPU-idle hint (when `nvidia-smi` is available) to encourage card utilization.
- [x] Keep “orchestrator runs forever” flow: queued messages are auto-injected into follow-up turns.

## P0: Ask-User / Interrupt Semantics
- [x] Fix `ask_user` waiting path so it consumes only real user messages.
- [x] Preserve non-user queue payloads (e.g., heartbeat system nudges) during `ask_user` waits.

## P0: Prompt Design (Execution Discipline)
- [x] Strengthen orchestrator prompt: worker-first execution, worker-report-then-verify pipeline.
- [x] Add explicit uv/venv execution preference to orchestrator + worker prompts.
- [x] Add anti-self-mention guidance in orchestrator prompt (avoid `@orchestrator` self-addressing).

## P1: Router Setup Behavior
- [x] Strengthen `/setup` and `/setup-project` injected prompts to require creating/updating `work_dir/CLAUDE.md` with filesystem/server/environment notes.
- [x] Keep router aware of `config.md` as user setup/environment notes.

## P1: Permission Handling
- [x] Remove hardcoded permission mode in daemon/router session runtime.
- [x] Add configurable `permission_mode` path (`config.json -> router -> daemon /task`).

## P1: Frontend Message Policy
- [x] Refine chat display rules to remove unnecessary self-mention prefixes while preserving monitor logs.
- [x] Keep worker-only internals in monitor unless intentionally surfaced as chat exchange.

## P2: Docs / Config Alignment
- [x] Update README for heartbeat behavior, `permission_mode`, and setup expectations.
- [x] Update `config.example.json` with `orchestrator.permission_mode` example.
