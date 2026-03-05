#!/usr/bin/env python3
"""Audit harness for 8-K Haiku prescreen decisions on S3 artifacts."""
from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path
import sys

import boto3

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src.modules.events.eight_k_scanner.models import ExtractedFiling


def _list_filing_prefixes(s3, bucket: str, limit: int) -> list[str]:
    prefixes: list[str] = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix="data/raw/filings/"):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.endswith("/extracted.json"):
                prefixes.append(key[: -len("/extracted.json")])
                if len(prefixes) >= limit:
                    return prefixes
    return prefixes


def _read_json(s3, bucket: str, key: str) -> dict:
    body = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
    return json.loads(body)


def _full_8k_text(extracted: ExtractedFiling) -> str:
    parts: list[str] = []
    for item_num, text in extracted.items.items():
        parts.append(f"Item {item_num}\n{text}")
    if not parts and extracted.text:
        parts.append(extracted.text)
    return "\n\n".join(parts).strip()


def main() -> None:
    parser = argparse.ArgumentParser(description="Audit 8-K Haiku prescreen decisions")
    parser.add_argument("--bucket", required=True, help="S3 bucket")
    parser.add_argument("--limit", type=int, default=200, help="Max extracted filings to sample")
    parser.add_argument("--out", default="workspace/8k_prescreen_audit.csv", help="CSV output path")
    args = parser.parse_args()

    s3 = boto3.client("s3")
    prefixes = _list_filing_prefixes(s3, args.bucket, args.limit)

    rows: list[dict[str, str]] = []
    for prefix in prefixes:
        try:
            index = _read_json(s3, args.bucket, f"{prefix}/index.json")
            form_type = (index.get("form_type") or "").upper()
            if form_type not in ("8-K", "8-K/A"):
                continue

            extracted_raw = _read_json(s3, args.bucket, f"{prefix}/extracted.json")
            extracted = ExtractedFiling.model_validate(extracted_raw)
            screening = _read_json(s3, args.bucket, f"{prefix}/screening.json")
            outcome = str(screening.get("outcome", "UNKNOWN")).upper()

            analysis_class = ""
            try:
                analysis = _read_json(s3, args.bucket, f"{prefix}/analysis.json")
                analysis_class = str(analysis.get("classification", ""))
            except Exception:
                pass

            full_text = _full_8k_text(extracted)
            rows.append(
                {
                    "prefix": prefix,
                    "ticker": index.get("ticker", ""),
                    "form_type": form_type,
                    "outcome": outcome,
                    "screen_chars": "",
                    "full_chars": str(len(full_text)),
                    "would_run_sonnet": "yes" if outcome == "POSITIVE" else "no",
                    "analysis_classification": analysis_class,
                }
            )
        except Exception:
            rows.append(
                {
                    "prefix": prefix,
                    "ticker": "",
                    "form_type": "",
                    "outcome": "ERROR",
                    "screen_chars": "0",
                    "full_chars": "0",
                    "would_run_sonnet": "unknown",
                    "analysis_classification": "",
                }
            )

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "prefix",
                "ticker",
                "form_type",
                "outcome",
                "screen_chars",
                "full_chars",
                "would_run_sonnet",
                "analysis_classification",
            ],
        )
        writer.writeheader()
        writer.writerows(rows)

    positives = sum(1 for r in rows if r["outcome"] == "POSITIVE")
    neutrals = sum(1 for r in rows if r["outcome"] == "NEUTRAL")
    negatives = sum(1 for r in rows if r["outcome"] == "NEGATIVE")
    errors = sum(1 for r in rows if r["outcome"] == "ERROR")

    print(f"rows={len(rows)} positives={positives} neutrals={neutrals} negatives={negatives} errors={errors}")
    print(f"csv={out_path}")


if __name__ == "__main__":
    main()
