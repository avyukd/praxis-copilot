"""Extract clean text from raw 8-K filing HTML stored in S3."""
from __future__ import annotations

import logging
import re

from bs4 import BeautifulSoup

from src.modules.events.eight_k_scanner.config import S3_BUCKET, S3_RAW_PREFIX
from src.modules.events.eight_k_scanner.models import ExtractedExhibit, ExtractedFiling
from src.modules.events.eight_k_scanner.storage.s3 import get_s3_client, read_json_from_s3, write_json_to_s3

logger = logging.getLogger(__name__)

# Item header pattern
ITEM_PATTERN = re.compile(
    r"(?:^|\n)\s*(?:Item|ITEM)\s+(\d+\.\d+)\s*[.:\-\u2013\u2014]?\s*",
)

SIGNATURE_PATTERNS = [
    re.compile(r"\n\s*SIGNATURE[S]?\s*\n", re.IGNORECASE),
    re.compile(r"\n\s*Pursuant to the requirements of the Securities Exchange Act", re.IGNORECASE),
]

EXHIBIT_INDEX_PATTERNS = [
    re.compile(r"\n\s*EXHIBIT\s+INDEX\s*\n", re.IGNORECASE),
    re.compile(r"\n\s*Exhibit\s+Number\s+", re.IGNORECASE),
]

ITEM_NOISE_PATTERNS = [
    re.compile(r"Interactive Data.*?(?=\n\s*(?:Item|ITEM)\s+\d+\.\d+|\Z)", re.DOTALL | re.IGNORECASE),
    re.compile(r"Document Format Files.*?(?=\n\s*(?:Item|ITEM)\s+\d+\.\d+|\Z)", re.DOTALL | re.IGNORECASE),
    re.compile(r"\nData Files\n.*?(?=\n\s*(?:Item|ITEM)\s+\d+\.\d+|\Z)", re.DOTALL | re.IGNORECASE),
    re.compile(r"Complete submission text file.*?\n", re.IGNORECASE),
    re.compile(r"Mailing Address\n.*?(?=Business Address|\n\n\n|\Z)", re.DOTALL | re.IGNORECASE),
    re.compile(r"Business Address\n.*?(?=\n\n\n|\n\s*(?:Item|ITEM)|\Z)", re.DOTALL | re.IGNORECASE),
    re.compile(r"\n.*?\(Filer\)\n.*?(?=\n\n\n|\Z)", re.DOTALL),
    re.compile(r"\nCIK\s*:.*?(?=\n\n\n|\Z)", re.DOTALL),
    re.compile(r"\nSIC\s*:.*?(?=\n\n\n|\Z)", re.DOTALL),
]

EXHIBIT_NOISE_PATTERNS = [
    re.compile(
        r"(?:Forward[- ]Looking Statements|FORWARD[- ]LOOKING STATEMENTS)\s*\n.*",
        re.DOTALL | re.IGNORECASE,
    ),
    re.compile(r"\n\s*About\s+[A-Z].*?(?=\n\s*Forward[- ]Looking|\n\s*Company Contact|\n\s*Investor Contact|\n\s*Media Contact|\n\s*Contact|\Z)", re.DOTALL),
    re.compile(r"\n\s*(?:Company|Investor|Media)\s+Contact:?\s*\n.*", re.DOTALL | re.IGNORECASE),
    re.compile(r"\n\s*Financial tables follow\s*\n.*", re.DOTALL | re.IGNORECASE),
]

SKIP_FILENAME_PATTERNS = [
    re.compile(r"-index", re.IGNORECASE),
    re.compile(r"^R\d+\.htm$", re.IGNORECASE),
]


def _should_skip_file(filename: str) -> bool:
    for pattern in SKIP_FILENAME_PATTERNS:
        if pattern.search(filename):
            return True
    return False


def extract_filing(
    cik: str,
    accession_number: str,
    bucket: str | None = None,
    prefix: str = S3_RAW_PREFIX,
    force: bool = False,
) -> ExtractedFiling | None:
    """Extract text from a raw filing in S3. Writes extracted.json back to S3.

    Returns the ExtractedFiling, or None if already extracted and not force.
    """
    bucket = bucket or S3_BUCKET
    filing_prefix = f"{prefix}/{cik}/{accession_number}"
    output_key = f"{filing_prefix}/extracted.json"

    if not force:
        try:
            get_s3_client().head_object(Bucket=bucket, Key=output_key)
            logger.info(f"Already extracted: {accession_number}")
            return None
        except Exception:
            pass

    index_key = f"{filing_prefix}/index.json"
    try:
        index_data = read_json_from_s3(bucket, index_key)
    except Exception as e:
        logger.error(f"Cannot read index for {accession_number}: {e}")
        return None

    primary_doc = index_data.get("primary_doc")
    exhibit_manifest = index_data.get("exhibit_manifest", [])
    ticker = index_data.get("ticker", "")

    items: dict[str, str] = {}
    files_processed = 0
    files_skipped = 0

    if primary_doc:
        primary_key = f"{filing_prefix}/{primary_doc}"
        try:
            content = _read_s3_file(bucket, primary_key)
            text = _parse_html(content)
            text = _strip_boilerplate(text)
            items = _split_items(text)
            items = {k: _strip_item_noise(v) for k, v in items.items()}
            items = {k: v for k, v in items.items() if v.strip()}
            files_processed += 1
        except Exception as e:
            logger.warning(f"Failed to parse primary doc {primary_doc}: {e}")
            files_skipped += 1
    else:
        logger.warning(f"No primary doc for {accession_number}")

    exhibits: list[ExtractedExhibit] = []
    for exhibit in exhibit_manifest:
        filename = exhibit.get("filename", "")
        exhibit_type = exhibit.get("type", "other")

        if not filename.lower().endswith((".htm", ".html")):
            files_skipped += 1
            continue
        if _should_skip_file(filename):
            files_skipped += 1
            continue
        if filename == primary_doc:
            files_skipped += 1
            continue

        exhibit_key = f"{filing_prefix}/{filename}"
        try:
            content = _read_s3_file(bucket, exhibit_key)
            text = _parse_html(content)
            text = _strip_exhibit_boilerplate(text)
            if text.strip():
                exhibits.append(ExtractedExhibit(
                    filename=filename,
                    type=exhibit_type,
                    text=text.strip(),
                ))
                files_processed += 1
            else:
                files_skipped += 1
        except Exception as e:
            logger.warning(f"Failed to parse exhibit {filename}: {e}")
            files_skipped += 1

    total_chars = sum(len(v) for v in items.values()) + sum(len(e.text) for e in exhibits)

    result = ExtractedFiling(
        cik=cik,
        accession_number=accession_number,
        ticker=ticker,
        items=items,
        exhibits=exhibits,
        total_chars=total_chars,
        files_processed=files_processed,
        files_skipped=files_skipped,
    )

    write_json_to_s3(bucket, output_key, result.model_dump())
    logger.info(f"Extracted {accession_number}: {files_processed} files, {total_chars} chars")
    return result


def _read_s3_file(bucket: str, key: str) -> str:
    resp = get_s3_client().get_object(Bucket=bucket, Key=key)
    raw = resp["Body"].read()
    try:
        return raw.decode("utf-8")
    except UnicodeDecodeError:
        return raw.decode("latin-1")


def _parse_html(content: str) -> str:
    soup = BeautifulSoup(content, "lxml")
    for tag in soup(["script", "style"]):
        tag.decompose()
    text = soup.get_text(separator="\n")
    text = re.sub(r"\n{3,}", "\n\n", text)
    text = re.sub(r"[ \t]{2,}", " ", text)
    lines = [line.strip() for line in text.split("\n")]
    return "\n".join(lines)


def _strip_boilerplate(text: str) -> str:
    item_match = ITEM_PATTERN.search(text)
    if item_match:
        text = text[item_match.start():]

    for pattern in SIGNATURE_PATTERNS:
        match = pattern.search(text)
        if match:
            text = text[:match.start()]
            break

    for pattern in EXHIBIT_INDEX_PATTERNS:
        match = pattern.search(text)
        if match:
            text = text[:match.start()]
            break

    return text.strip()


def _strip_item_noise(text: str) -> str:
    for pattern in ITEM_NOISE_PATTERNS:
        text = pattern.sub("", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def _strip_exhibit_boilerplate(text: str) -> str:
    text = re.sub(
        r"^(?:EX-\d+\.\d+\n.*?\n.*?\n(?:EXHIBIT\s+\d+\.\d+\n)?)",
        "", text, count=1, flags=re.IGNORECASE,
    )
    text = re.sub(r"^(?:Exhibit\s+\d+\.\d+)\s*\n", "", text, count=1, flags=re.IGNORECASE)

    for pattern in EXHIBIT_NOISE_PATTERNS:
        text = pattern.sub("", text)

    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def extract_generic_filing(
    cik: str,
    accession_number: str,
    bucket: str | None = None,
    prefix: str = "data/raw/filings",
    form_type: str = "",
    force: bool = False,
) -> ExtractedFiling | None:
    """Extract text from a non-8-K filing (10-K, 10-Q, etc.) in S3.

    Simpler than 8-K extraction: just HTML -> plain text, no item splitting.
    Writes extracted.json back to S3.
    """
    bucket = bucket or S3_BUCKET
    file_prefix = f"{prefix}/{cik}/{accession_number}"
    output_key = f"{file_prefix}/extracted.json"

    if not force:
        try:
            get_s3_client().head_object(Bucket=bucket, Key=output_key)
            logger.info(f"Already extracted: {accession_number}")
            return None
        except Exception:
            pass

    index_key = f"{file_prefix}/index.json"
    try:
        index_data = read_json_from_s3(bucket, index_key)
    except Exception as e:
        logger.error(f"Cannot read index for {accession_number}: {e}")
        return None

    primary_doc = index_data.get("primary_doc")
    ticker = index_data.get("ticker", "")
    total_chars = 0
    full_text = ""

    if primary_doc:
        primary_key = f"{file_prefix}/{primary_doc}"
        try:
            content = _read_s3_file(bucket, primary_key)
            full_text = _parse_html(content)
            total_chars = len(full_text)
        except Exception as e:
            logger.warning(f"Failed to parse primary doc {primary_doc}: {e}")

    result = ExtractedFiling(
        cik=cik,
        accession_number=accession_number,
        ticker=ticker,
        form_type=form_type,
        items={},
        exhibits=[],
        text=full_text,
        total_chars=total_chars,
        files_processed=1 if full_text else 0,
        files_skipped=0,
    )

    write_json_to_s3(bucket, output_key, result.model_dump())
    logger.info(f"Extracted {form_type} {accession_number}: {total_chars} chars")
    return result


def _split_items(text: str) -> dict[str, str]:
    splits = list(ITEM_PATTERN.finditer(text))

    if not splits:
        if text.strip():
            return {"unknown": text.strip()}
        return {}

    items: dict[str, str] = {}
    for i, match in enumerate(splits):
        item_num = match.group(1)
        start = match.end()
        end = splits[i + 1].start() if i + 1 < len(splits) else len(text)
        item_text = text[start:end].strip()
        if item_text:
            items[item_num] = item_text

    return items
