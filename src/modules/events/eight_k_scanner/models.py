"""Pydantic models for the 8-K scanner module.

Replaces untyped dicts flowing between poller, fetcher, extractor, analyzer,
and alerting subsystems.
"""
from __future__ import annotations

from pydantic import BaseModel, Field


# ---------------------------------------------------------------------------
# EDGAR poller / filing metadata
# ---------------------------------------------------------------------------


class ExhibitManifestEntry(BaseModel):
    filename: str
    type: str = "other"


class FilingMeta(BaseModel):
    """Metadata stored as index.json in S3 for an 8-K filing."""

    cik: str
    accession_number: str
    primary_doc: str | None = None
    items_detected: list[str] = Field(default_factory=list)
    acceptance_datetime: str = ""
    exhibit_manifest: list[ExhibitManifestEntry] = Field(default_factory=list)

    # Enriched by poller_handler after universe check
    ticker: str = ""
    company_name: str = ""
    market_cap: int | None = None
    exchange: str = ""
    filed_date: str = ""
    source: str = ""

    # Timestamps set during processing
    extracted_at: str | None = None
    analyzed_at: str | None = None
    alert_sent_at: str | None = None


class FilingDocument(BaseModel):
    """Result of fetching a filing's raw documents from EDGAR."""

    metadata: FilingMeta
    documents: dict[str, bytes] = Field(default_factory=dict)

    class Config:
        arbitrary_types_allowed = True


# ---------------------------------------------------------------------------
# Poller output (what EFTS / RSS returns before enrichment)
# ---------------------------------------------------------------------------


class PolledFiling(BaseModel):
    """A filing discovered by the EDGAR poller (EFTS or RSS)."""

    cik: str
    accession_number: str
    company_name: str = ""
    ticker: str = ""
    form_type: str = ""
    filed_date: str = ""
    acceptance_datetime: str = ""
    items: list[str] = Field(default_factory=list)


# ---------------------------------------------------------------------------
# Extraction
# ---------------------------------------------------------------------------


class ExtractedExhibit(BaseModel):
    filename: str
    type: str = "other"
    text: str


class ExtractedFiling(BaseModel):
    """Output of the HTML parser / item splitter (extracted.json)."""

    cik: str
    accession_number: str
    ticker: str = ""
    form_type: str = ""
    items: dict[str, str] = Field(default_factory=dict)
    exhibits: list[ExtractedExhibit] = Field(default_factory=list)
    text: str = ""  # plain text for non-8-K filings (10-K, 10-Q)
    total_chars: int = 0
    files_processed: int = 0
    files_skipped: int = 0


# ---------------------------------------------------------------------------
# Financial snapshot
# ---------------------------------------------------------------------------


class FinancialSnapshot(BaseModel):
    market_cap: int | None = None
    revenue_ttm: int | None = None
    net_income_ttm: int | None = None
    cash: int | None = None
    total_debt: int | None = None
    source: str = "unknown"


# ---------------------------------------------------------------------------
# Universe info
# ---------------------------------------------------------------------------


class UniverseInfo(BaseModel):
    ticker: str = ""
    company_name: str = ""
    market_cap: int | None = None
    exchange: str = ""
    symbol: str = ""


# ---------------------------------------------------------------------------
# Newswire / press release
# ---------------------------------------------------------------------------


class PressRelease(BaseModel):
    """A press release discovered from a newswire feed."""

    release_id: str
    title: str = ""
    url: str = ""
    published_at: str = ""
    source: str = ""
    ticker: str = ""
    exchange: str = ""


class FetchedRelease(BaseModel):
    """Result of fetching full press release text."""

    text: str
    metadata: dict[str, str] = Field(default_factory=dict)


# ---------------------------------------------------------------------------
# Press release index (stored as index.json for CA / US PR)
# ---------------------------------------------------------------------------


class PRIndexMeta(BaseModel):
    """Metadata stored as index.json for a press release in S3."""

    ticker: str
    symbol: str = ""
    exchange: str = ""
    market_cap: int | None = None
    release_id: str
    title: str = ""
    url: str = ""
    published_at: str = ""
    filed_date: str = ""
    acceptance_datetime: str = ""
    source: str = ""
    extracted_at: str | None = None
    analyzed_at: str | None = None
    alert_sent_at: str | None = None
