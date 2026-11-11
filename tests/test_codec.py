import pytest

from weather_station.codec import (
    PAYLOAD_SIZE,
    decode_auto,
    decode_compact,
    decode_json,
    encode_compact,
    encode_json,
)
from weather_station.sensor import WIND_DIRECTIONS, Reading, WeatherSensor


@pytest.mark.parametrize("reading", [
    Reading(0.0, 0, "N"),
    Reading(110.0, 100, "NE"),
    Reading(56.32, 51, "SO"),
    Reading(0.01, 1, "E"),
    Reading(109.99, 99, "O"),
])
def test_compact_roundtrip_edges(reading):
    payload = encode_compact(reading)
    assert len(payload) == PAYLOAD_SIZE
    assert decode_compact(payload) == reading


def test_compact_roundtrip_all_wind_directions():
    for d in WIND_DIRECTIONS:
        r = Reading(55.55, 55, d)
        assert decode_compact(encode_compact(r)) == r


def test_compact_roundtrip_random_readings():
    sensor = WeatherSensor(seed=1234)
    for _ in range(5000):
        r = sensor.read()
        assert decode_compact(encode_compact(r)) == r


def test_json_roundtrip():
    r = Reading(56.32, 51, "SO")
    assert decode_json(encode_json(r)) == r


def test_decode_auto_detects_format():
    r = Reading(23.45, 67, "S")
    assert decode_auto(encode_compact(r)) == r
    assert decode_auto(encode_json(r)) == r


def test_decode_compact_rejects_wrong_size():
    with pytest.raises(ValueError):
        decode_compact(b"\x00\x00")
