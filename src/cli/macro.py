"""Macro workspace utilities — sync a local directory of macro notes to/from S3."""

from pathlib import Path

from cli.s3 import BUCKET, download_file, list_prefix, upload_directory

MACRO_PREFIX = "data/context/macro/"


def list_macro_files(s3_client) -> list[str]:
    """List all macro files on S3. Returns relative paths (e.g. 'rates-higher-longer.md')."""
    keys = list_prefix(s3_client, MACRO_PREFIX)
    return [k[len(MACRO_PREFIX):] for k in keys if k != MACRO_PREFIX]


def pull_macro_workspace(s3_client, local_dir: Path) -> list[str]:
    """Download all macro files from S3 to local_dir. Returns list of relative paths pulled."""
    keys = list_prefix(s3_client, MACRO_PREFIX)
    pulled = []
    for key in keys:
        relative = key[len(MACRO_PREFIX):]
        if not relative:
            continue
        local_path = local_dir / relative
        local_path.parent.mkdir(parents=True, exist_ok=True)
        content = download_file(s3_client, key)
        local_path.write_bytes(content)
        pulled.append(relative)
    return pulled


def sync_macro_workspace(s3_client, local_dir: Path) -> list[str]:
    """Upload all files from local_dir to S3 under macro prefix. Returns uploaded keys."""
    return upload_directory(s3_client, local_dir, MACRO_PREFIX.rstrip("/"))
