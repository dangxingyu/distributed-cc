"""In-memory data models for work plans and task decomposition."""

from dataclasses import dataclass, field


@dataclass
class WorkItem:
    id: str                        # e.g. "t1", "t2"
    description: str               # Human-readable goal
    server: str
    session: str
    prompt: str                    # Detailed prompt for broker
    status: str = "pending"        # pending | running | done | failed
    depends_on: list[str] = field(default_factory=list)
    result: str | None = None
    feedback: str | None = None    # From verification (for retry)
    retries: int = 0
    max_retries: int = 2
    approach_changes: int = 0
    max_approach_changes: int = 1


@dataclass
class WorkPlan:
    id: str
    chat_id: int
    user_message: str
    items: list[WorkItem] = field(default_factory=list)
    status: str = "active"         # active | completed | failed
