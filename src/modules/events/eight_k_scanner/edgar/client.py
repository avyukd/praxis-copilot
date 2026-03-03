"""EDGAR HTTP client with rate limiting and retry."""
from __future__ import annotations

import logging
import time

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from src.modules.events.eight_k_scanner.config import SEC_USER_AGENT, EDGAR_MAX_RPS

logger = logging.getLogger(__name__)

_last_request_time = 0.0
_min_interval = 1.0 / EDGAR_MAX_RPS


def _rate_limit():
    global _last_request_time
    now = time.monotonic()
    elapsed = now - _last_request_time
    if elapsed < _min_interval:
        time.sleep(_min_interval - elapsed)
    _last_request_time = time.monotonic()


def _build_session():
    session = requests.Session()
    session.headers.update({
        "User-Agent": SEC_USER_AGENT,
        "Accept-Encoding": "gzip, deflate",
    })
    retry = Retry(
        total=3,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
    )
    session.mount("https://", HTTPAdapter(max_retries=retry))
    return session


_session = None


def get_session():
    global _session
    if _session is None:
        _session = _build_session()
    return _session


def edgar_get(url: str, **kwargs):
    _rate_limit()
    resp = get_session().get(url, timeout=30, **kwargs)
    resp.raise_for_status()
    return resp
