"""Shared LLM call logic: throttling, retry, Sonnet convenience wrapper."""
from __future__ import annotations

import logging
import random
import time
from threading import Lock

from litellm import completion

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
MAX_RETRIES = 4
BASE_BACKOFF_SECONDS = 1.5
DEFAULT_MODEL = "anthropic/claude-sonnet-4-5-20250929"
HAIKU_MODEL = "anthropic/claude-haiku-4-5-20251001"
TOKENS_PER_MINUTE = 30_000

_rate_lock = Lock()
_window_start = 0.0
_tokens_used = 0


def _estimate_tokens(text: str) -> int:
    return max(1, len(text) // 4) + 1000


def _throttle(text: str) -> None:
    global _window_start, _tokens_used
    if TOKENS_PER_MINUTE <= 0:
        return

    tokens_needed = min(TOKENS_PER_MINUTE, _estimate_tokens(text))

    while True:
        with _rate_lock:
            now = time.monotonic()
            elapsed = now - _window_start
            if elapsed >= 60 or _window_start == 0.0:
                _window_start = now
                _tokens_used = 0

            if _tokens_used + tokens_needed <= TOKENS_PER_MINUTE:
                _tokens_used += tokens_needed
                return

            wait_for = max(0.0, 60 - elapsed)
        logger.debug("LLM rate limit reached. Sleeping %.1fs", wait_for)
        time.sleep(wait_for if wait_for > 0 else 1.0)


def call_sonnet(
    system: str,
    user: str,
    model: str | None = None,
) -> str:
    """Call Sonnet with system + user message. Returns response text.

    Includes rate limiting and retry logic.
    """
    model = model or DEFAULT_MODEL
    messages = [
        {"role": "system", "content": system},
        {"role": "user", "content": user},
    ]

    _throttle(system + user)

    last_error: Exception | None = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = completion(model=model, messages=messages)
            return response.choices[0].message.content
        except RETRIABLE_EXCEPTIONS as exc:
            last_error = exc
            if attempt == MAX_RETRIES:
                raise
            sleep_for = BASE_BACKOFF_SECONDS * (2 ** (attempt - 1)) + random.uniform(0, 0.5)
            logger.warning(
                "LLM call failed (attempt %s/%s): %s -- retrying in %.1fs",
                attempt, MAX_RETRIES, exc, sleep_for,
            )
            time.sleep(sleep_for)
        except APIError:
            raise

    raise RuntimeError("LLM call loop exited unexpectedly") from last_error


def call_haiku(
    system: str,
    user: str,
    model: str | None = None,
) -> str:
    """Call Haiku with system + user message. Returns response text.

    Same retry/throttle logic as call_sonnet but defaults to Haiku model.
    """
    return call_sonnet(system=system, user=user, model=model or HAIKU_MODEL)
