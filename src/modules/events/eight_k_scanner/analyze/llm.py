"""LLM-based 8-K filing analysis via LiteLLM."""
from __future__ import annotations

import logging
import random
import time
from threading import Lock
from typing import Literal

from pydantic import BaseModel, Field
from litellm import completion

from src.modules.events.eight_k_scanner.analyze.prompt import build_messages
from src.modules.events.eight_k_scanner.config import LLM_MODEL, LLM_TOKENS_PER_MINUTE

logger = logging.getLogger(__name__)

try:
    from litellm.exceptions import (
        APIError,
        APIConnectionError,
        RateLimitError,
        ServiceUnavailableError,
        APITimeoutError,
    )
except Exception:
    APIError = APIConnectionError = RateLimitError = ServiceUnavailableError = APITimeoutError = Exception  # type: ignore

RETRIABLE_EXCEPTIONS = (
    RateLimitError,
    ServiceUnavailableError,
    APITimeoutError,
    APIConnectionError,
)
MAX_LLM_RETRIES = 4
BASE_BACKOFF_SECONDS = 1.5

_rate_lock = Lock()
_window_start = 0.0
_tokens_used = 0


def _estimate_tokens(messages: list[dict]) -> int:
    total_chars = sum(len(msg.get("content", "")) for msg in messages)
    prompt_tokens = max(1, total_chars // 4)
    return prompt_tokens + 1000


def _throttle_llm(messages: list[dict]) -> None:
    global _window_start, _tokens_used
    if LLM_TOKENS_PER_MINUTE <= 0:
        return

    tokens_needed = min(LLM_TOKENS_PER_MINUTE, _estimate_tokens(messages))

    while True:
        with _rate_lock:
            now = time.monotonic()
            elapsed = now - _window_start
            if elapsed >= 60 or _window_start == 0.0:
                _window_start = now
                _tokens_used = 0

            if _tokens_used + tokens_needed <= LLM_TOKENS_PER_MINUTE:
                _tokens_used += tokens_needed
                return

            wait_for = max(0.0, 60 - elapsed)
        logger.debug(
            "LLM rate limit reached (used=%s, needed=%s). Sleeping %.1fs",
            _tokens_used, tokens_needed, wait_for,
        )
        time.sleep(wait_for if wait_for > 0 else 1.0)


class AnalysisResult(BaseModel):
    classification: Literal["BUY", "SELL", "NEUTRAL"]
    magnitude: float = Field(ge=0.0, le=1.0)
    new_information: str
    materiality: str
    explanation: str


class ScreeningResult(BaseModel):
    classification: Literal["BUY", "SELL", "NEUTRAL"]
    magnitude: float = Field(ge=0.0, le=1.0)


class TokenUsage(BaseModel):
    input_tokens: int | None = None
    output_tokens: int | None = None
    total_tokens: int | None = None
    usage_available: bool = False
    usage_warning: str | None = None


class AnalysisWithUsage(BaseModel):
    analysis: AnalysisResult
    token_usage: TokenUsage


def _extract_token_usage(response) -> TokenUsage:
    usage = getattr(response, "usage", None)
    if usage is None and isinstance(response, dict):
        usage = response.get("usage")

    if usage is None:
        return TokenUsage(
            usage_available=False,
            usage_warning="Provider did not return token usage",
        )

    if isinstance(usage, dict):
        prompt = usage.get("prompt_tokens")
        compl = usage.get("completion_tokens")
        total = usage.get("total_tokens")
    else:
        prompt = getattr(usage, "prompt_tokens", None)
        compl = getattr(usage, "completion_tokens", None)
        total = getattr(usage, "total_tokens", None)

    if prompt is None and compl is None and total is None:
        return TokenUsage(
            usage_available=False,
            usage_warning="Provider usage object missing token counts",
        )

    return TokenUsage(
        input_tokens=prompt,
        output_tokens=compl,
        total_tokens=total,
        usage_available=True,
    )


def _sum_optional_ints(*values: int | None) -> int | None:
    present = [v for v in values if v is not None]
    return sum(present) if present else None


def _combine_token_usage(*usages: TokenUsage) -> TokenUsage:
    if usages and all(u.usage_available for u in usages):
        return TokenUsage(
            input_tokens=_sum_optional_ints(*(u.input_tokens for u in usages)),
            output_tokens=_sum_optional_ints(*(u.output_tokens for u in usages)),
            total_tokens=_sum_optional_ints(*(u.total_tokens for u in usages)),
            usage_available=True,
        )

    warnings = [u.usage_warning for u in usages if u.usage_warning]
    return TokenUsage(
        usage_available=False,
        usage_warning="; ".join(warnings) if warnings else "Token usage unavailable",
    )


def _call_llm_with_usage(
    *,
    model: str,
    messages: list[dict],
    ticker: str,
    response_format,
) -> tuple[str, TokenUsage]:
    _throttle_llm(messages)

    last_error: Exception | None = None
    for attempt in range(1, MAX_LLM_RETRIES + 1):
        try:
            response = completion(
                model=model,
                messages=messages,
                response_format=response_format,
            )
            return response.choices[0].message.content, _extract_token_usage(response)
        except RETRIABLE_EXCEPTIONS as exc:
            last_error = exc
            if attempt == MAX_LLM_RETRIES:
                logger.error(
                    "LLM call exhausted retries (%s attempts) for %s",
                    MAX_LLM_RETRIES, ticker,
                )
                raise

            sleep_for = BASE_BACKOFF_SECONDS * (2 ** (attempt - 1))
            sleep_for += random.uniform(0, 0.5)
            logger.warning(
                "LLM call failed (attempt %s/%s) for %s: %s -- retrying in %.1fs",
                attempt, MAX_LLM_RETRIES, ticker, exc, sleep_for,
            )
            time.sleep(sleep_for)
        except APIError:
            raise

    raise RuntimeError("LLM call loop exited unexpectedly") from last_error


def analyze_filing(
    extracted: dict,
    financial_snapshot: dict,
    ticker: str,
    model: str | None = None,
    messages: list[dict] | None = None,
) -> AnalysisResult:
    return analyze_filing_with_usage(
        extracted=extracted,
        financial_snapshot=financial_snapshot,
        ticker=ticker,
        model=model,
        messages=messages,
    ).analysis


def analyze_filing_with_usage(
    extracted: dict,
    financial_snapshot: dict,
    ticker: str,
    model: str | None = None,
    messages: list[dict] | None = None,
) -> AnalysisWithUsage:
    """Run LLM analysis on an extracted filing or press release.

    Two-stage: screening first, full analysis only if BUY.
    """
    model = model or LLM_MODEL
    if messages is None:
        messages = build_messages(extracted, financial_snapshot, ticker)

    accession = extracted.get("accession_number", "?")
    logger.info(f"Calling LLM screen ({model}) for {ticker} / {accession}")
    screen_content, screen_usage = _call_llm_with_usage(
        model=model,
        messages=messages,
        ticker=ticker,
        response_format=ScreeningResult,
    )
    screen = ScreeningResult.model_validate_json(screen_content)

    if screen.classification != "BUY":
        logger.info(
            "LLM screen result: %s magnitude=%s",
            screen.classification, screen.magnitude,
        )
        return AnalysisWithUsage(
            analysis=AnalysisResult(
                classification=screen.classification,
                magnitude=screen.magnitude,
                new_information="",
                materiality="",
                explanation="",
            ),
            token_usage=screen_usage,
        )

    logger.info(f"Calling LLM full analysis ({model}) for {ticker} / {accession}")
    content, full_usage = _call_llm_with_usage(
        model=model,
        messages=messages,
        ticker=ticker,
        response_format=AnalysisResult,
    )
    result = AnalysisResult.model_validate_json(content)
    usage = _combine_token_usage(screen_usage, full_usage)
    logger.info(f"LLM result: {result.classification} magnitude={result.magnitude}")
    return AnalysisWithUsage(analysis=result, token_usage=usage)
