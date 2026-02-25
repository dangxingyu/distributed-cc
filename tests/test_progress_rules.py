from src.progress_rules import chat_messages_from_event, log_entries_from_event


def test_chat_message_strips_orchestrator_sender_prefix_for_worker_assignment():
    msgs = chat_messages_from_event(
        "text",
        "@orchestrator -> @worker: run pytest -q",
    )
    assert msgs == [("orchestrator", "@worker: run pytest -q")]


def test_chat_message_strips_worker_sender_prefix():
    msgs = chat_messages_from_event(
        "text",
        "@worker -> @orchestrator: tests passed",
    )
    assert msgs == [("worker", "@orchestrator: tests passed")]


def test_done_message_no_self_mention_prefix():
    msgs = chat_messages_from_event("done", "ship it")
    assert msgs == [("orchestrator", "Task complete: ship it")]


def test_error_message_no_self_mention_prefix():
    msgs = chat_messages_from_event("error", "SDK crashed")
    assert msgs == [("orchestrator", "Error: SDK crashed")]


def test_orchestrator_bracket_message_still_surfaces_clean_text():
    msgs = chat_messages_from_event("text", "[orchestrator] Investigating now.")
    assert msgs == [("orchestrator", "Investigating now.")]


def test_log_entries_keep_text_events():
    entries = log_entries_from_event("text", "[orchestrator] hello")
    assert entries == ["[orchestrator] hello"]
