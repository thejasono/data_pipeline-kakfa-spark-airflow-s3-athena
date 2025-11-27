"""Unit tests for the Kafka streaming transform helpers."""

from __future__ import annotations

import sys
import uuid
from pathlib import Path

import pytest


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from dags.producer.kafka_streaming_service import (
    _fallback_user_data,
    encrypt_zip,
    safe_float,
    transform_user_data,
)


@pytest.fixture(autouse=True)
def block_network(monkeypatch: pytest.MonkeyPatch) -> None:
    """Prevent accidental outbound HTTP calls during tests."""

    def _blocked(*_args, **_kwargs):  # pragma: no cover - defensive only
        raise AssertionError("HTTP requests are blocked in tests")

    monkeypatch.setattr("dags.producer.kafka_streaming_service.requests.get", _blocked)


@pytest.fixture()
def deterministic_fallback(monkeypatch: pytest.MonkeyPatch) -> None:
    """Make fallback generation reproducible for assertions."""

    monkeypatch.setattr("dags.producer.kafka_streaming_service.time.time_ns", lambda: 123_456_789)
    monkeypatch.setattr(
        "dags.producer.kafka_streaming_service.uuid.uuid4",
        lambda: uuid.UUID("12345678-1234-5678-1234-567812345678"),
    )


@pytest.mark.parametrize(
    "value,expected",
    [
        ("12.34", 12.34),
        (42, 42.0),
        ("not-a-number", None),
        (None, None),
    ],
)
def test_safe_float_handles_valid_and_invalid_values(value, expected):
    assert safe_float(value) == expected


def test_transform_user_data_normalizes_payload_and_hashes_zip():
    raw = {
        "name": {"title": "Ms", "first": "Jane", "last": "Doe"},
        "gender": "female",
        "location": {
            "street": {"number": 42, "name": "Example Road"},
            "city": "Sampletown",
            "country": "Wonderland",
            "postcode": "90210",
            "coordinates": {"latitude": "34.09", "longitude": "-118.41"},
        },
        "email": "jane.doe@example.com",
    }

    result = transform_user_data(raw)

    assert result == {
        "name": "Ms. Jane Doe",
        "gender": "female",
        "address": "42, Example Road",
        "city": "Sampletown",
        "nation": "Wonderland",
        "zip": str(encrypt_zip("90210")),
        "latitude": 34.09,
        "longitude": -118.41,
        "email": "jane.doe@example.com",
    }


def test_transform_user_data_handles_missing_fields_and_bad_coordinates():
    raw = {
        "name": {"title": "Dr", "first": "Alex", "last": "Smith"},
        "location": {
            "street": {"number": 7, "name": "Testing Lane"},
            "city": "Nowhere",
            "country": "Noland",
            "postcode": "NW1 6XE",
            "coordinates": {"latitude": "n/a", "longitude": "also bad"},
        },
    }

    result = transform_user_data(raw)

    assert result["gender"] is None
    assert result["email"] is None
    assert result["zip"] == str(encrypt_zip("NW1 6XE"))
    assert result["latitude"] is None
    assert result["longitude"] is None
    assert result["name"] == "Dr. Alex Smith"
    assert result["address"] == "7, Testing Lane"


def test_fallback_user_data_is_reproducible_and_transformable(deterministic_fallback):
    fallback = _fallback_user_data()

    assert fallback["gender"] in {"male", "female"}
    assert fallback["name"]["first"] and fallback["name"]["last"]
    assert fallback["location"]["city"]

    transformed = transform_user_data(fallback)

    assert transformed["zip"] == str(encrypt_zip("M5V"))
    assert transformed["email"].endswith("@example.com")
    assert transformed["latitude"] == pytest.approx(43.6470)
    assert transformed["longitude"] == pytest.approx(-79.3948)
    assert transformed["name"].startswith("Ms. Sophia")
