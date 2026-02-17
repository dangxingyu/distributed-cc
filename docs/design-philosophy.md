# Design Philosophy: Professor → PhD Student → Claude Code

## The Analogy

The system mirrors an academic research lab:

- **User = Hands-on Professor** — Gives high-level ideas with specific intuitions. Opinionated and inspiring, but doesn't micromanage. If the student doesn't ask, the professor trusts them to figure it out.
- **Orchestrator = PhD Student** — The autonomous researcher. Receives the professor's direction, thinks about how to approach it, decomposes into concrete tasks for their tools (workers), reviews results, and iterates. If the professor doesn't respond to a question, the student continues and makes their own judgment call.
- **Workers = Claude Code instances** — The student's implementation tools. Given specific, actionable assignments. The student reviews their reports, interacts during execution, and verifies results.

## Example Flow

**Professor says:**
> "Investigating why training loss plateaus — I think maybe it's reward hacking since the reward isn't quite robust, please check it out."

**PhD Student (orchestrator) thinks:**
The professor has a specific hypothesis (reward hacking). The codebase is verl. I need to:
1. Check if we even have reward logging to see patterns
2. Look at actual training logs for evidence

**Student assigns to workers:**
- Worker 1: "Implement reward logging in verl so we can track reward distributions over training"
- Worker 2: "Go to training log xxx.json, look at the later examples, and check if there's any obvious reward hacking pattern — e.g. reward going up but output quality degrading"

**Key points:**
- Sometimes the professor's instruction is specific enough to pass almost directly to a single worker
- Sometimes the student needs to decompose a bit — but into substantial chunks, not micro-steps
- The student never decomposes into things like "write tests" or "update docstring" — those are within a worker's scope

## Task Granularity

Each worker task should be something a capable Claude Code session can own end-to-end:
- "Implement reward logging in verl" (hours of work, clear goal)
- "Analyze training logs for reward hacking patterns" (substantial investigation)
- NOT: "add a print statement", "run pytest", "update the README"

The student (orchestrator) does the thinking about what to investigate and how to decompose.
The professor (user) provides direction and judgment.
The workers (Claude Code) execute.

## Escalation Philosophy

- The student asks the professor when they genuinely need advice or a decision (design choices, interpreting ambiguous results, prioritization)
- If the professor doesn't respond, the student makes their best judgment and keeps going
- Routine tool permissions and implementation details are the student's call, not the professor's

## Channel Model

Each project is like a Slack channel with three participants: user, orchestrator, and workers.

```
#project-backend
  user: investigate why the API is slow, might be the DB queries
  orchestrator: Looking into it. Assigning to worker on prod server.
  orchestrator -> prod/backend: Profile the /users endpoint, check for N+1 queries
  prod/backend: Done. Found 3 N+1 queries in users handler. Fixed. Response time 2s→80ms.
  orchestrator: Task complete — N+1 queries fixed, API response time improved 25x.
  orchestrator: Suggested next steps: Run load tests to verify under production traffic.
```

Key principles:
- **Everything visible** — the user sees orchestrator↔worker exchanges, not just final results
- **One persistent session** — the orchestrator is a single Claude session (via `--resume`) that accumulates context across all interactions, not a pipeline of separate calls
- **Workers get curated context** — workers see the orchestrator's prompt, not the full channel history. The orchestrator's job is to distill relevant context.
- **Natural decision-making** — instead of separate verify/reflect/suggest pipelines, the orchestrator sees worker results and naturally decides: accept, retry, try differently, escalate, or suggest next steps
