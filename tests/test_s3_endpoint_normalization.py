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
        ("https://s3.us-east-1.amazonaws.com", "s3.us-east-1.amazonaws.com", True),
        ("http://s3.us-west-2.amazonaws.com", "s3.us-west-2.amazonaws.com", False),
        ("s3.eu-west-2.amazonaws.com", "s3.eu-west-2.amazonaws.com", None),
        ("S3.CUSTOM-ENDPOINT.EXAMPLE.COM", "S3.CUSTOM-ENDPOINT.EXAMPLE.COM", None),
        ("https://objects.example.com:8443", "objects.example.com:8443", True),
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
        "https://s3.us-east-1.amazonaws.com/bucket",  # unexpected path
        "http://s3.us-east-1.amazonaws.com/",  # trailing slash should fail fast
        "s3.us-east-1.amazonaws.com/",  # trailing slash without scheme
        "s3.us-east-1.amazonaws.com/extra",  # path without scheme
        "http://s3.us-east-1.amazonaws.com?foo=bar",  # query parameters are not supported
    ],
)
def test_normalize_invalid_inputs(raw):
    with pytest.raises(ValueError):
        _normalize_s3_endpoint(raw)
