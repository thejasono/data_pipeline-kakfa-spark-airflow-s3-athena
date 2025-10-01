"""Checks for configuration defaults that must remain stable."""
from __future__ import annotations

from pathlib import Path


def _load_env_value(key: str) -> str:
    env_path = Path(__file__).resolve().parent.parent / ".env"
    for raw_line in env_path.read_text().splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith(f"{key}="):
            return line.split("=", 1)[1]
    raise AssertionError(f"Missing {key} entry in .env")


def test_s3_endpoint_default_targets_aws_regional_host() -> None:
    value = _load_env_value("S3_ENDPOINT")
    assert value == "https://s3.eu-west-2.amazonaws.com"

