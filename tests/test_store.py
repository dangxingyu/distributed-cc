"""Test JSON file store — messages, channels, notes, logs, and project mapping."""

import asyncio
import json
import os
import pytest

from src.store import Store


@pytest.fixture
async def store(tmp_path):
    s = Store(str(tmp_path / "data"))
    await s.init()
    yield s
    await s.close()


# -- messages ---------------------------------------------------------


async def test_add_and_get_messages(store):
    await store.add_message(1, "user", "hello")
    await store.add_message(1, "assistant", "hi there")
    await store.add_message(1, "user", "how are you")

    msgs = await store.get_recent_messages(1)
    assert len(msgs) == 3
    assert msgs[0]["role"] == "user"
    assert msgs[0]["content"] == "hello"
    assert msgs[0]["ts"] is not None
    assert msgs[1]["role"] == "assistant"
    assert msgs[2]["content"] == "how are you"


async def test_messages_per_chat(store):
    await store.add_message(1, "user", "chat 1")
    await store.add_message(2, "user", "chat 2")

    msgs1 = await store.get_recent_messages(1)
    msgs2 = await store.get_recent_messages(2)
    assert len(msgs1) == 1
    assert len(msgs2) == 1
    assert msgs1[0]["content"] == "chat 1"
    assert msgs2[0]["content"] == "chat 2"


async def test_message_all_returned(store):
    for i in range(30):
        await store.add_message(1, "user", f"msg {i}")

    msgs = await store.get_recent_messages(1)
    assert len(msgs) == 30
    assert msgs[0]["content"] == "msg 0"
    assert msgs[-1]["content"] == "msg 29"


async def test_concurrent_message_writes_do_not_drop_entries(store):
    async def _write(i: int):
        await store.add_message(1, "user", f"msg-{i}")

    await asyncio.gather(*[_write(i) for i in range(80)])
    msgs = await store.get_recent_messages(1)

    assert len(msgs) == 80
    assert {m["content"] for m in msgs} == {f"msg-{i}" for i in range(80)}


async def test_get_recent_messages_empty(store):
    msgs = await store.get_recent_messages(999)
    assert msgs == []


# -- channels ---------------------------------------------------------


async def test_create_channel(store):
    ch_id = await store.create_channel("my-project")
    assert ch_id >= 1
    channels = await store.get_channel_list()
    assert any(c["id"] == ch_id and c["name"] == "my-project" for c in channels)


async def test_create_channel_with_project_mapping(store):
    ch_id = await store.create_channel("with-project", project_id="proj-a")
    assert await store.get_channel_project(ch_id) == "proj-a"


async def test_channel_list_sorted(store):
    id1 = await store.create_channel("alpha")
    id2 = await store.create_channel("beta")
    id3 = await store.create_channel("gamma")
    channels = await store.get_channel_list()
    assert len(channels) == 3
    assert channels[0]["id"] == id1
    assert channels[1]["id"] == id2
    assert channels[2]["id"] == id3


async def test_channel_list_empty(store):
    channels = await store.get_channel_list()
    assert channels == []


async def test_delete_channel(store):
    ch_id = await store.create_channel("temp")
    await store.delete_channel(ch_id)
    channels = await store.get_channel_list()
    assert len(channels) == 0


async def test_get_all_channel_ids(store):
    id1 = await store.create_channel("one")
    id2 = await store.create_channel("two")
    ids = await store.get_all_channel_ids()
    assert set(ids) == {id1, id2}


async def test_concurrent_channel_creation_has_unique_ids(store):
    ids = await asyncio.gather(*[store.create_channel(f"ch-{i}") for i in range(30)])
    assert len(ids) == 30
    assert len(set(ids)) == 30


async def test_set_get_channel_project_map(store):
    id1 = await store.create_channel("one")
    id2 = await store.create_channel("two")

    await store.set_channel_project(id1, "proj-a")
    await store.set_channel_project(id2, "proj-b")

    mapping = await store.get_channel_project_map()
    assert mapping[id1] == "proj-a"
    assert mapping[id2] == "proj-b"


async def test_set_channel_project_none_clears(store):
    """Setting project to None clears the mapping."""
    ch_id = await store.create_channel("clearable", project_id="proj-a")
    assert await store.get_channel_project(ch_id) == "proj-a"

    await store.set_channel_project(ch_id, None)
    assert await store.get_channel_project(ch_id) is None

    # Should not appear in the map
    mapping = await store.get_channel_project_map()
    assert ch_id not in mapping


async def test_set_channel_project_on_missing_channel_is_noop(tmp_path):
    data_dir = tmp_path / "data"
    s = Store(str(data_dir))
    await s.init()

    missing_id = 42
    channel_path = data_dir / "channels" / f"{missing_id}.json"
    assert not channel_path.exists()

    await s.set_channel_project(missing_id, None)
    assert not channel_path.exists()

    await s.close()


# -- notes ------------------------------------------------------------


async def test_add_and_get_notes(store):
    await store.add_note(1, "remember this")
    notes = await store.get_unchecked_notes(1)
    assert len(notes) == 1
    assert notes[0]["content"] == "remember this"


async def test_mark_notes_checked(store):
    await store.add_note(1, "note1")
    await store.add_note(1, "note2")
    assert len(await store.get_unchecked_notes(1)) == 2
    await store.mark_notes_checked(1)
    assert len(await store.get_unchecked_notes(1)) == 0


async def test_notes_isolated_per_channel(store):
    await store.add_note(1, "note-for-1")
    await store.add_note(2, "note-for-2")
    assert len(await store.get_unchecked_notes(1)) == 1
    assert len(await store.get_unchecked_notes(2)) == 1


# -- logs -------------------------------------------------------------


async def test_add_and_get_logs(store):
    await store.add_log(1, "tool call: Bash")
    await store.add_log(1, "tool call: Read")
    logs = await store.get_logs(1)
    assert len(logs) == 2
    assert logs[0]["text"] == "tool call: Bash"


async def test_empty_logs(store):
    logs = await store.get_logs(999)
    assert logs == []


# -- persistence ------------------------------------------------------


async def test_persistence_across_instances(tmp_path):
    data_dir = str(tmp_path / "data")

    s1 = Store(data_dir)
    await s1.init()
    chat_id = await s1.create_channel("persistent", project_id="proj-a")
    await s1.add_message(chat_id, "user", "saved")
    await s1.close()

    s2 = Store(data_dir)
    await s2.init()

    msgs = await s2.get_recent_messages(chat_id)
    assert len(msgs) == 1
    assert msgs[0]["content"] == "saved"
    assert await s2.get_channel_project(chat_id) == "proj-a"
    await s2.close()


async def test_backward_compat_missing_fields(tmp_path):
    """Old channel files may lack meta/notes/logs/project_id fields."""
    data_dir = str(tmp_path / "data")
    store = Store(data_dir)
    await store.init()

    channel_path = os.path.join(data_dir, "channels", "1.json")
    with open(channel_path, "w") as f:
        json.dump({"messages": [{"role": "user", "content": "old"}]}, f)

    msgs = await store.get_recent_messages(1)
    assert len(msgs) == 1
    notes = await store.get_unchecked_notes(1)
    assert notes == []
    logs = await store.get_logs(1)
    assert logs == []
    assert await store.get_channel_project(1) is None
    await store.close()


async def test_json_file_structure(store, tmp_path):
    await store.add_message(42, "user", "hi")

    path = tmp_path / "data" / "channels" / "42.json"
    assert path.exists()

    with open(path) as f:
        data = json.load(f)

    assert "messages" in data
    assert len(data["messages"]) == 1
    assert data["messages"][0]["content"] == "hi"
    assert "meta" in data
    assert "project_id" in data["meta"]


# -- source field -----------------------------------------------------


async def test_create_channel_with_source(store):
    ch_id = await store.create_channel("web-chan", source="web")
    src = await store.get_channel_source(ch_id)
    assert src == "web"


async def test_create_channel_default_source_is_none(store):
    ch_id = await store.create_channel("no-source")
    src = await store.get_channel_source(ch_id)
    assert src is None


async def test_set_channel_source(store):
    ch_id = await store.create_channel("tagged")
    assert await store.get_channel_source(ch_id) is None

    await store.set_channel_source(ch_id, "telegram")
    assert await store.get_channel_source(ch_id) == "telegram"


async def test_set_channel_source_missing_channel_is_noop(store):
    # Should not crash or create a file
    await store.set_channel_source(9999, "web")
    src = await store.get_channel_source(9999)
    assert src is None


async def test_ensure_channel_creates_file_with_source(store):
    await store.ensure_channel(98765, name="tg chat", source="telegram")
    assert await store.get_channel_source(98765) == "telegram"


async def test_get_channel_list_filtered_by_source(store):
    id_web = await store.create_channel("web-chan", source="web")
    id_tg = await store.create_channel("tg-chan", source="telegram")
    id_legacy = await store.create_channel("legacy-chan")  # source=None

    # All channels
    all_channels = await store.get_channel_list()
    assert len(all_channels) == 3

    # Web (explicit): only web
    web_channels = await store.get_channel_list(source="web")
    web_ids = {c["id"] for c in web_channels}
    assert id_web in web_ids
    assert id_legacy not in web_ids
    assert id_tg not in web_ids

    # Web (+legacy opt-in): web + legacy
    web_plus_legacy = await store.get_channel_list(source="web", include_legacy=True)
    web_plus_legacy_ids = {c["id"] for c in web_plus_legacy}
    assert id_web in web_plus_legacy_ids
    assert id_legacy in web_plus_legacy_ids
    assert id_tg not in web_plus_legacy_ids

    # Telegram: telegram only
    tg_channels = await store.get_channel_list(source="telegram")
    tg_ids = {c["id"] for c in tg_channels}
    assert id_tg in tg_ids
    assert id_legacy not in tg_ids
    assert id_web not in tg_ids


async def test_get_channel_project_map_by_source(store):
    id_web = await store.create_channel("web-chan", project_id="proj-a", source="web")
    id_tg = await store.create_channel("tg-chan", project_id="proj-a", source="telegram")
    id_legacy = await store.create_channel("legacy-chan", project_id="proj-b")

    web_map = await store.get_channel_project_map_by_source("web")
    assert id_web in web_map
    assert id_legacy not in web_map
    assert id_tg not in web_map

    web_map_with_legacy = await store.get_channel_project_map_by_source("web", include_legacy=True)
    assert id_web in web_map_with_legacy
    assert id_legacy in web_map_with_legacy
    assert id_tg not in web_map_with_legacy

    tg_map = await store.get_channel_project_map_by_source("telegram")
    assert id_tg in tg_map
    assert id_legacy not in tg_map
    assert id_web not in tg_map


async def test_backward_compat_source_field_missing(tmp_path):
    """Old channel files without source field get source=None."""
    data_dir = str(tmp_path / "data")
    store = Store(data_dir)
    await store.init()

    channel_path = os.path.join(data_dir, "channels", "1.json")
    with open(channel_path, "w") as f:
        json.dump({
            "meta": {"name": "old", "project_id": "proj"},
            "messages": [],
            "notes": [],
            "logs": [],
        }, f)

    src = await store.get_channel_source(1)
    assert src is None
    await store.close()
