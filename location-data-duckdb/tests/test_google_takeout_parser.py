from location_pipeline.sources.google_takeout import _e7_to_float, _parse_ts_millis


def test_e7_conversion() -> None:
    assert _e7_to_float(377699999) == 37.7699999


def test_parse_timestamp_ms() -> None:
    dt = _parse_ts_millis("1700000000000")
    assert dt is not None
    assert dt.year == 2023
