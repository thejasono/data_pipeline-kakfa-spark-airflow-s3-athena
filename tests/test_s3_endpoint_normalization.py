from pathlib import Path
import sys

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from spark.app.spark_processing import _normalize_s3_endpoint


@pytest.mark.parametrize(
    "raw,expected_host,expected_ssl",
    [
        ("http://minio:9000", "minio:9000", False),
        ("https://minio:9000", "minio:9000", True),
        ("minio:9000", "minio:9000", None),
        ("MINIO:9000", "MINIO:9000", None),
        ("https://play.min.io:9443", "play.min.io:9443", True),
    ],
)
def test_normalize_valid_inputs(raw, expected_host, expected_ssl):
    host, ssl_enabled = _normalize_s3_endpoint(raw)
    assert host == expected_host
    assert ssl_enabled is expected_ssl


@pytest.mark.parametrize(
    "raw",
    [
        "",  # empty string
        "   ",  # whitespace only
        None,  # explicit None should raise ValueError
        "http://",  # missing host
        "https://minio:9000/bucket",  # unexpected path
        "http://minio:9000/",  # trailing slash should fail fast
        "minio:9000/",  # trailing slash without scheme
        "minio:9000/extra",  # path without scheme
        "http://minio:9000?foo=bar",  # query parameters are not supported
    ],
)
def test_normalize_invalid_inputs(raw):
    with pytest.raises(ValueError):
        _normalize_s3_endpoint(raw)
