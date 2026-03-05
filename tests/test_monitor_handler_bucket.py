from src.modules.monitor.evaluator import handler


class _FakeBody:
    def __init__(self, text: str):
        self._text = text

    def read(self):
        return self._text.encode("utf-8")


class _FakePaginator:
    def paginate(self, Bucket, Prefix):
        assert Bucket == "test-bucket"
        assert Prefix == "config/monitors/"
        return [{"Contents": [{"Key": "config/monitors/sample.yaml"}]}]


class _FakeS3:
    def get_paginator(self, name):
        assert name == "list_objects_v2"
        return _FakePaginator()

    def get_object(self, Bucket, Key):
        assert Bucket == "test-bucket"
        assert Key == "config/monitors/sample.yaml"
        return {"Body": _FakeBody("id: sample\ntype: filing\ntickers: [AGM]\ndescription: d\nextract: e\n")}


def test_load_monitor_configs_uses_configured_bucket(monkeypatch):
    monkeypatch.setattr(handler, "BUCKET", "test-bucket")
    configs = handler._load_monitor_configs(_FakeS3())
    assert len(configs) == 1
    assert configs[0].id == "sample"
