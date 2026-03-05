from types import SimpleNamespace

from src.modules.events.eight_k_scanner import poller_handler


def _filing(cik: str, accession: str, form_type: str):
    return SimpleNamespace(
        cik=cik,
        accession_number=accession,
        form_type=form_type,
        ticker="SMSI",
        company_name="Smith Micro Software, Inc.",
        filed_date="2026-03-05",
        acceptance_datetime="2026-03-05T13:00:00Z",
    )


def _fetch_result():
    return SimpleNamespace(
        metadata=SimpleNamespace(
            ticker=None,
            company_name=None,
            market_cap=None,
            exchange=None,
            filed_date=None,
            acceptance_datetime=None,
            model_dump=lambda: {"ticker": "SMSI"},
        ),
        documents={"primary.htm": b"<html>ok</html>"},
    )


def test_non_8k_requires_monitor_subscription(monkeypatch):
    filings = [_filing("1234", "a10k", "10-K")]
    stored = []

    monkeypatch.setattr(poller_handler, "_load_monitor_subscriptions", lambda: {})
    monkeypatch.setattr(poller_handler, "poll_new_8k_filings", lambda lookback_minutes, forms=None: filings)
    monkeypatch.setattr(
        poller_handler,
        "is_in_universe",
        lambda cik: (True, SimpleNamespace(ticker="SMSI", company_name="Smith", market_cap=100.0, exchange="NASDAQ")),
    )
    monkeypatch.setattr(poller_handler, "_filing_exists_unified", lambda cik, accession: False)
    monkeypatch.setattr(poller_handler, "fetch_filing", lambda cik, accession: _fetch_result())
    monkeypatch.setattr(poller_handler, "_store_filing_unified", lambda cik, accession, index_data, docs: stored.append(accession))
    monkeypatch.setattr(poller_handler, "SCANNER_LOOKBACK_MINUTES", 60)

    result = poller_handler.lambda_handler()

    assert result["stored"] == 0
    assert result["filtered_out"] == 1
    assert stored == []


def test_non_8k_with_monitor_subscription_is_stored(monkeypatch):
    filings = [_filing("1234", "a10k", "10-K")]
    stored = []

    monkeypatch.setattr(poller_handler, "_load_monitor_subscriptions", lambda: {"1234": ["10-K"]})
    monkeypatch.setattr(poller_handler, "poll_new_8k_filings", lambda lookback_minutes, forms=None: filings)
    monkeypatch.setattr(
        poller_handler,
        "is_in_universe",
        lambda cik: (False, SimpleNamespace(ticker="", company_name="", market_cap=None, exchange="")),
    )
    monkeypatch.setattr(poller_handler, "_filing_exists_unified", lambda cik, accession: False)
    monkeypatch.setattr(poller_handler, "fetch_filing", lambda cik, accession: _fetch_result())
    monkeypatch.setattr(poller_handler, "_store_filing_unified", lambda cik, accession, index_data, docs: stored.append(accession))
    monkeypatch.setattr(poller_handler, "SCANNER_LOOKBACK_MINUTES", 60)

    result = poller_handler.lambda_handler()

    assert result["stored"] == 1
    assert result["filtered_out"] == 0
    assert stored == ["a10k"]


def test_8k_behavior_unchanged_for_in_universe(monkeypatch):
    filings = [_filing("1234", "a8k", "8-K")]
    stored = []

    monkeypatch.setattr(poller_handler, "_load_monitor_subscriptions", lambda: {})
    monkeypatch.setattr(poller_handler, "poll_new_8k_filings", lambda lookback_minutes, forms=None: filings)
    monkeypatch.setattr(
        poller_handler,
        "is_in_universe",
        lambda cik: (True, SimpleNamespace(ticker="SMSI", company_name="Smith", market_cap=100.0, exchange="NASDAQ")),
    )
    monkeypatch.setattr(poller_handler, "_filing_exists_unified", lambda cik, accession: False)
    monkeypatch.setattr(poller_handler, "fetch_filing", lambda cik, accession: _fetch_result())
    monkeypatch.setattr(poller_handler, "_store_filing_unified", lambda cik, accession, index_data, docs: stored.append(accession))
    monkeypatch.setattr(poller_handler, "SCANNER_LOOKBACK_MINUTES", 60)

    result = poller_handler.lambda_handler()

    assert result["stored"] == 1
    assert result["filtered_out"] == 0
    assert stored == ["a8k"]
