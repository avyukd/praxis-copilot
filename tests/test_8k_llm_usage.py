from types import SimpleNamespace

from src.modules.events.eight_k_scanner.analyze import llm
from src.modules.events.eight_k_scanner.models import ExtractedFiling, FinancialSnapshot


def _fake_response(content: str, usage):
    message = SimpleNamespace(content=content)
    choice = SimpleNamespace(message=message)
    return SimpleNamespace(choices=[choice], usage=usage)


def _base_extracted(accession: str) -> ExtractedFiling:
    return ExtractedFiling(cik="0000000000", accession_number=accession)


def test_analyze_filing_with_usage_includes_token_counts(monkeypatch):
    calls = []
    responses = [
        _fake_response(
            '{"classification":"BUY","magnitude":0.6}',
            {"prompt_tokens": 10, "completion_tokens": 2, "total_tokens": 12},
        ),
        _fake_response(
            '{"classification":"BUY","magnitude":0.6,"new_information":"x","materiality":"y","explanation":"z"}',
            {"prompt_tokens": 123, "completion_tokens": 45, "total_tokens": 168},
        ),
    ]

    def fake_completion(**kwargs):
        calls.append(kwargs.get("response_format").__name__)
        return responses.pop(0)

    monkeypatch.setattr(llm, "completion", fake_completion)
    monkeypatch.setattr(llm, "_throttle_llm", lambda messages: None)

    result = llm.analyze_filing_with_usage(
        extracted=_base_extracted("0001"),
        financial_snapshot=FinancialSnapshot(),
        ticker="TEST",
        messages=[{"role": "user", "content": "hello"}],
    )

    assert result.analysis.classification == "BUY"
    assert result.analysis.explanation == "z"
    assert calls == ["ScreeningResult", "AnalysisResult"]
    assert result.token_usage.usage_available is True
    assert result.token_usage.input_tokens == 133
    assert result.token_usage.output_tokens == 47
    assert result.token_usage.total_tokens == 180
    assert result.token_usage.usage_warning is None


def test_analyze_filing_with_usage_marks_missing_usage(monkeypatch):
    calls = []

    def fake_completion(**kwargs):
        calls.append(kwargs.get("response_format").__name__)
        return _fake_response(
            '{"classification":"NEUTRAL","magnitude":0.1}',
            None,
        )

    monkeypatch.setattr(llm, "completion", fake_completion)
    monkeypatch.setattr(llm, "_throttle_llm", lambda messages: None)

    result = llm.analyze_filing_with_usage(
        extracted=_base_extracted("0002"),
        financial_snapshot=FinancialSnapshot(),
        ticker="TEST2",
        messages=[{"role": "user", "content": "hello"}],
    )

    assert result.analysis.classification == "NEUTRAL"
    assert result.analysis.new_information == ""
    assert result.analysis.materiality == ""
    assert result.analysis.explanation == ""
    assert calls == ["ScreeningResult"]
    assert result.token_usage.usage_available is False
    assert result.token_usage.input_tokens is None
    assert result.token_usage.output_tokens is None
    assert result.token_usage.total_tokens is None
    assert result.token_usage.usage_warning == "Provider did not return token usage"


def test_analyze_filing_with_usage_supports_object_usage(monkeypatch):
    usage_obj = SimpleNamespace(prompt_tokens=7, completion_tokens=3, total_tokens=10)
    calls = []

    def fake_completion(**kwargs):
        calls.append(kwargs.get("response_format").__name__)
        return _fake_response(
            '{"classification":"SELL","magnitude":0.4}',
            usage_obj,
        )

    monkeypatch.setattr(llm, "completion", fake_completion)
    monkeypatch.setattr(llm, "_throttle_llm", lambda messages: None)

    result = llm.analyze_filing_with_usage(
        extracted=_base_extracted("0003"),
        financial_snapshot=FinancialSnapshot(),
        ticker="TEST3",
        messages=[{"role": "user", "content": "hello"}],
    )

    assert result.analysis.classification == "SELL"
    assert result.analysis.explanation == ""
    assert calls == ["ScreeningResult"]
    assert result.token_usage.usage_available is True
    assert result.token_usage.input_tokens == 7
    assert result.token_usage.output_tokens == 3
    assert result.token_usage.total_tokens == 10
    assert result.token_usage.usage_warning is None
