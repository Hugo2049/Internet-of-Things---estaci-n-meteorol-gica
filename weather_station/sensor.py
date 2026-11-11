"""Simulacion de los sensores de la estacion meteorologica."""

from __future__ import annotations

import random
from dataclasses import asdict, dataclass

WIND_DIRECTIONS: tuple[str, ...] = ("N", "NO", "O", "SO", "S", "SE", "E", "NE")

TEMP_MIN, TEMP_MAX = 0.0, 110.0
HUMIDITY_MIN, HUMIDITY_MAX = 0, 100

MEAN = 55.0
DEFAULT_STDDEV = 15.0  # ~99.7 % de las muestras queda en [10, 100]


@dataclass(frozen=True)
class Reading:
    temperatura: float
    humedad: int
    direccion_viento: str

    def __post_init__(self) -> None:
        if not TEMP_MIN <= self.temperatura <= TEMP_MAX:
            raise ValueError(f"temperatura fuera de rango: {self.temperatura}")
        if not HUMIDITY_MIN <= self.humedad <= HUMIDITY_MAX:
            raise ValueError(f"humedad fuera de rango: {self.humedad}")
        if self.direccion_viento not in WIND_DIRECTIONS:
            raise ValueError(f"direccion de viento invalida: {self.direccion_viento}")

    def to_dict(self) -> dict:
        return asdict(self)


def _clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


class WeatherSensor:
    """Genera lecturas con distribucion normal (media 55) acotada a cada rango."""

    def __init__(self, stddev: float = DEFAULT_STDDEV, seed: int | None = None) -> None:
        self._stddev = stddev
        self._rng = random.Random(seed)

    def read(self) -> Reading:
        temperatura = round(_clamp(self._rng.gauss(MEAN, self._stddev), TEMP_MIN, TEMP_MAX), 2)
        humedad = int(round(_clamp(self._rng.gauss(MEAN, self._stddev), HUMIDITY_MIN, HUMIDITY_MAX)))
        direccion = self._rng.choice(WIND_DIRECTIONS)
        return Reading(temperatura, humedad, direccion)
