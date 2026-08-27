"""Banned vocabulary for the business-neutrality gate (single source of truth).

The task-status words are the union of the framework default vocabularies
(orditect-core TaskStatus + orditect-flow TaskStatus). They are legal inside
the core/flow packages (they ARE the framework's own defaults) and illegal
on the contract surface of orditect-protocol (T6: vocabulary neutrality).
The business-concept words are ecosystem/business terms that must never leak
into the protocol contract surface either.
"""

TASK_STATUS_WORDS: frozenset[str] = frozenset({
    "pending", "queued", "running", "in_progress",
    "completed", "succeeded", "failed", "cancelled",
})

BUSINESS_CONCEPT_WORDS: frozenset[str] = frozenset({
    "llm", "agent", "budget", "sse", "webhook", "websocket",
    "langchain", "langgraph", "autogen", "deepagent", "mcp",
})

ALL_BANNED: frozenset[str] = TASK_STATUS_WORDS | BUSINESS_CONCEPT_WORDS