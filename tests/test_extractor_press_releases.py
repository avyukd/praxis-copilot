from src.modules.events.eight_k_scanner import extractor_handler


class _FakeBody:
    def __init__(self, data: bytes):
        self._data = data

    def read(self):
        return self._data


class _FakeS3:
    def __init__(self):
        self.objects = {
            "data/raw/press_releases/gnw/NVDA/r1/release.txt": b"Launch announced.",
        }

    def get_object(self, Bucket, Key):
        return {"Body": _FakeBody(self.objects[Key])}


def test_extract_press_release_writes_extracted_payload(monkeypatch):
    writes = []
    monkeypatch.setattr(extractor_handler, "get_s3_client", lambda: _FakeS3())
    monkeypatch.setattr(
        extractor_handler,
        "write_json_to_s3",
        lambda bucket, key, payload: writes.append((bucket, key, payload)),
    )

    result = extractor_handler._extract_press_release_one(
        bucket="praxis-copilot",
        source="gnw",
        ticker="NVDA",
        release_id="r1",
    )

    assert result["action"] == "extracted"
    assert writes
    _, key, payload = writes[0]
    assert key == "data/raw/press_releases/gnw/NVDA/r1/extracted.json"
    assert payload["source_type"] == "press_releases"
    assert payload["ticker"] == "NVDA"
    assert payload["text"] == "Launch announced."
