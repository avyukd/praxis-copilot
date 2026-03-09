import sys
from pathlib import Path

import yaml
from click.testing import CliRunner

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from cli.main import cli


def test_status_reuses_monitor_scan_and_research_listing(monkeypatch, tmp_path):
    config_dir = tmp_path / "config"
    config_dir.mkdir()
    (config_dir / "universe.yaml").write_text(
        yaml.safe_dump({"tickers": ["AAPL", "MSFT"]}),
        encoding="utf-8",
    )

    calls: list[str] = []
    monitor_docs = {
        "config/monitors/aapl.yaml": {
            "tickers": ["AAPL"],
            "listen": ["AAPL:filing", "AAPL:search", "MSFT:search"],
        },
        "config/monitors/msft.yaml": {
            "tickers": ["MSFT"],
            "listen": ["MSFT:filing"],
        },
    }
    research_keys = {
        "data/research/AAPL/": [
            "data/research/AAPL/memo.yaml",
            "data/research/AAPL/data/financials.json",
            "data/research/AAPL/data/transcript.json",
        ],
        "data/research/MSFT/": [
            "data/research/MSFT/data/financials.json",
        ],
    }

    def fake_list_prefix(_s3, prefix):
        calls.append(prefix)
        if prefix == "config/monitors/":
            return list(monitor_docs)
        return research_keys.get(prefix, [])

    def fake_download_file(_s3, key):
        return yaml.safe_dump(monitor_docs[key]).encode("utf-8")

    monkeypatch.setattr("cli.main.get_config_dir", lambda: config_dir)
    monkeypatch.setattr("cli.main.get_s3_client", lambda: object())
    monkeypatch.setattr("cli.main.list_prefix", fake_list_prefix)
    monkeypatch.setattr("cli.main.download_file", fake_download_file)

    result = CliRunner().invoke(cli, ["status"])

    assert result.exit_code == 0
    assert "AAPL  memo:yes  data:2 files  monitors:1" in result.output
    assert "MSFT  memo:no  data:1 files  monitors:2" in result.output
    # Verify monitors were scanned only once (not per-ticker)
    assert calls.count("config/monitors/") == 1
    assert calls.count("data/research/AAPL/") == 1
    assert calls.count("data/research/MSFT/") == 1
