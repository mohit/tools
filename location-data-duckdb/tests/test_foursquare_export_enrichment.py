from pathlib import Path

from location_pipeline.sources.foursquare_export import load_foursquare_export


def test_enriches_missing_coords_and_uses_cache(monkeypatch, tmp_path: Path) -> None:
    csv_path = tmp_path / "checkins.csv"
    csv_path.write_text(
        "checkin_id,created_at,venue_id,venue_name\n"
        "chk-1,2024-10-22T10:00:00Z,4b0588caf964a52058cc22e3,Venue One\n",
        encoding="utf-8",
    )

    cache_path = tmp_path / "fsq-cache.json"
    call_count = {"value": 0}

    class Response:
        ok = True

        @staticmethod
        def json() -> dict:
            return {
                "geocodes": {"main": {"latitude": 40.7128, "longitude": -74.006}},
                "location": {
                    "address": "123 Test St",
                    "locality": "New York",
                    "country": "US",
                },
            }

    def fake_get(_url: str, *, headers: dict, timeout: float):
        call_count["value"] += 1
        assert headers["Authorization"] == "test-api-key"
        assert timeout == 5.0
        return Response()

    monkeypatch.setattr("location_pipeline.sources.foursquare_export.requests.get", fake_get)

    visits = load_foursquare_export(
        str(tmp_path),
        places_api_key="test-api-key",
        cache_path=cache_path,
        request_timeout_seconds=5.0,
    )

    assert len(visits) == 1
    assert visits[0].lat == 40.7128
    assert visits[0].lon == -74.006
    assert visits[0].payload["venue_location"]["city"] == "New York"
    assert call_count["value"] == 1
    assert cache_path.exists()

    def fail_get(*_args, **_kwargs):
        raise AssertionError("network call should not be made when cache exists")

    monkeypatch.setattr("location_pipeline.sources.foursquare_export.requests.get", fail_get)

    cached_visits = load_foursquare_export(
        str(tmp_path),
        places_api_key="test-api-key",
        cache_path=cache_path,
    )
    assert cached_visits[0].lat == 40.7128
    assert cached_visits[0].lon == -74.006


def test_does_not_call_places_api_without_place_id(monkeypatch, tmp_path: Path) -> None:
    csv_path = tmp_path / "checkins.csv"
    csv_path.write_text(
        "checkin_id,created_at,venue_name\n"
        "chk-1,2024-10-22T10:00:00Z,Venue One\n",
        encoding="utf-8",
    )

    called = {"value": False}

    def fake_get(*_args, **_kwargs):
        called["value"] = True
        raise AssertionError("request should not be sent")

    monkeypatch.setattr("location_pipeline.sources.foursquare_export.requests.get", fake_get)

    visits = load_foursquare_export(
        str(tmp_path),
        places_api_key="test-api-key",
        cache_path=tmp_path / "cache.json",
    )

    assert len(visits) == 1
    assert visits[0].lat is None
    assert visits[0].lon is None
    assert called["value"] is False
