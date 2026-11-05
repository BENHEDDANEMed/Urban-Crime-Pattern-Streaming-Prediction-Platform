"""Test du mode replay avec un faux Kafka (on garde les messages dans une liste)."""
import json

from producer.replay import replay


class FakeSender:
    dry_run = True

    def __init__(self):
        self.messages = []

    def send(self, topic, key, value):
        self.messages.append((topic, key, value))


def test_replay_sets_ingested_at_and_new_ids(tmp_path):
    path = tmp_path / "sample.jsonl"
    path.write_text(json.dumps({"event_id": "chicago-1", "city": "chicago", "ingested_at": None}) + "\n")

    sender = FakeSender()
    sent = replay(sender, str(path), speed=0, loop=True, max_events=3)

    assert sent == 3
    topic, key, value = sender.messages[0]
    assert topic == "crimes.raw" and key == "chicago"
    assert value["ingested_at"].endswith("Z")
    assert value["ingest_mode"] == "replay"
    ids = [m[2]["event_id"] for m in sender.messages]
    assert ids == ["chicago-1", "chicago-1-r2", "chicago-1-r3"]