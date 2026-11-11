import statistics

import pytest

from weather_station.sensor import WIND_DIRECTIONS, Reading, WeatherSensor


def test_readings_within_ranges_and_types():
    sensor = WeatherSensor(seed=42)
    for _ in range(5000):
        r = sensor.read()
        assert 0 <= r.temperatura <= 110 and round(r.temperatura, 2) == r.temperatura
        assert isinstance(r.humedad, int) and 0 <= r.humedad <= 100
        assert r.direccion_viento in WIND_DIRECTIONS


def test_distribution_centered_on_55():
    sensor = WeatherSensor(seed=7)
    readings = [sensor.read() for _ in range(20000)]
    assert statistics.mean(r.temperatura for r in readings) == pytest.approx(55, abs=0.5)
    assert statistics.mean(r.humedad for r in readings) == pytest.approx(55, abs=0.5)


def test_reading_validates_ranges():
    with pytest.raises(ValueError):
        Reading(110.01, 50, "N")
    with pytest.raises(ValueError):
        Reading(50.0, 101, "N")
    with pytest.raises(ValueError):
        Reading(50.0, 50, "X")
