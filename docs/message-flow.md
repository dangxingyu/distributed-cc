# Channel Message Flow

How messages travel through the system: user input → router → daemon → progress events → chat panel + monitor.

## 1. User sends a message

```
Browser → WS {"type":"message"} → WebChat → Store (persist as "user") → Router
```

**Router decision tree** (based on `orch.status`):

| Status | What happens |
|--------|-------------|
| No project connected | Reply: "Use `/connect <project-id>`" |
| `/connect`, `/stop`, `/status` | Handle command locally |
| `stuck` | POST `/interrupt` to daemon (answers `ask_user`) |
| `idle`, `done`, `error`, `unknown` | POST `/task` to daemon (starts new task) |
| `running` + `@orchestrator` prefix | POST `/interrupt` to daemon (urgent interrupt) |
| `running` + normal message | Queued as deferred task |

Router replies (e.g. "queued as next task", "stopping...") go through `send_reply` → persisted as `"assistant"` message → WS `"reply"` to channel viewers.

## 2. Daemon emits progress events

Events arrive via SSE or HTTP callback → `router.ingest_progress_event` → `web._handle_progress` → `_persist_and_emit_progress`.

| Daemon event | Chat panel (persisted) | Monitor (persisted log) | WS-only (ephemeral) |
|-------------|----------------------|------------------------|-------------------|
| `text` with `@orchestrator`/`@worker` prefix | assistant message + `reply` WS | log entry + `log` WS | — |
| `text` without prefix | — | log entry + `log` WS | — |
| `tool_use` | — | `→ {data}` log + `log` WS | — |
| `tool_error` | — | `[ERROR] {data}` log + `log` WS | — |
| `iteration` | — | — | `progress` WS (updates badge) |
| `done` | `@orchestrator Task complete: {summary}` | — | `progress` WS with `status:done` |
| `stuck` | `@orchestrator Needs input: {question}` | — | `progress` WS with `status:stuck` |
| `error` | `@orchestrator Error: {msg}` | `[ERROR] {msg}` log | `progress` WS with `status:error` |
| `task_list` | — | — | `task_list` WS (updates task list panel) |

Every event also triggers a `channel_status` WS broadcast to all clients (updates sidebar status dots).

## 3. What the daemon emits

From `orchestrator_daemon.py`, events emitted during a task:

| When | Event type | `data` format |
|------|-----------|--------------|
| Task starts | `iteration` | `Starting task: {text}` |
| `assign_worker` called | `iteration` | `Worker assignment 2/20` |
| `assign_worker` called | `tool_use` | `[orchestrator -> worker] {task}` |
| `assign_worker` called | `text` | `@orchestrator -> @worker: {task}` |
| Worker completes | `text` | `@worker -> @orchestrator: {report}` |
| Orchestrator thinks | `text` | `[orchestrator] {thinking text}` |
| Orchestrator uses a tool | `tool_use` | `[orchestrator] Read: {...}` |
| Worker uses a tool | `tool_use` | `[worker] Bash: {...}` |
| Tool error | `tool_error` | `[source] {error}` |
| `update_task_list` | `task_list` | markdown checkbox content |
| `ask_user` | `stuck` | the question text |
| `task_complete` | `done` | the summary text |
| Exception | `error` | error message |

## 4. What appears in the chat panel

Only messages with `@orchestrator` or `@worker` prefixes make it to chat:

| Message | Sender label | Color |
|---------|-------------|-------|
| `@orchestrator -> @worker: {task}` | Orchestrator | green |
| `@worker -> @orchestrator: {report}` | Worker | orange |
| `@orchestrator Task complete: {summary}` | Orchestrator | green |
| `@orchestrator Needs input: {question}` | Orchestrator | green |
| `@orchestrator Error: {msg}` | Orchestrator | green |
| `(queued as next task...)` | System | grey italic |
| `(urgent interrupt queued...)` | System | grey italic |
| User messages | You | purple |

Internal orchestrator reasoning (`[orchestrator] thinking...`) goes to the monitor log only — chat stays clean with just the orchestrator↔worker exchanges and status updates.

## 5. Frontend WS message types

Messages the frontend receives and how they're handled:

| WS type | Handler |
|---------|---------|
| `channel_switched` | Update active channel, load history/logs/members |
| `progress` | Update project badge, typing indicator; trigger stuck UX if `status:stuck` |
| `channel_status` | Update sidebar status dots for all channels |
| `task_list` | Render orchestrator's research plan in monitor task list panel |
| `log` | Append entry to monitor panel |
| `reply` | Classify (`@worker` → worker, `(...)` → system, else → orchestrator) and render in chat |
| `error` | Render as system message in chat |

## 6. Persistence

| Data | Where stored | Survives reload? |
|------|-------------|-----------------|
| User messages | `data/channels/{id}.json` messages[] | Yes |
| Assistant replies | `data/channels/{id}.json` messages[] | Yes |
| Monitor logs | `data/channels/{id}.json` logs[] | Yes |
| Task list | WS-only (ephemeral) | No (re-emitted on next update) |
| Iteration progress | WS-only (ephemeral) | No |
| Channel-project mapping | `data/channels/{id}.json` meta.project_id | Yes |
