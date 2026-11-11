"""Codificacion de lecturas: JSON (fase 3.2) y compacta de 3 bytes (fase 3.4).

Formato compacto (24 bits, big-endian):

    | temperatura x100 (14 bits) | humedad (7 bits) | viento (3 bits) |
      bits 23..10                  bits 9..3          bits 2..0

La temperatura tiene dos decimales en [0, 110.00]; multiplicada por 100 queda
como entero en [0, 11000], que cabe en 14 bits porque 2**14 = 16384.
"""

from __future__ import annotations

import json

from .sensor import WIND_DIRECTIONS, Reading

PAYLOAD_SIZE = 3

TEMP_BITS, HUMIDITY_BITS, WIND_BITS = 14, 7, 3
TEMP_SCALE = 100

_HUMIDITY_SHIFT = WIND_BITS
_TEMP_SHIFT = HUMIDITY_BITS + WIND_BITS
_HUMIDITY_MASK = (1 << HUMIDITY_BITS) - 1
_WIND_MASK = (1 << WIND_BITS) - 1
_TEMP_MASK = (1 << TEMP_BITS) - 1

assert TEMP_BITS + HUMIDITY_BITS + WIND_BITS == PAYLOAD_SIZE * 8
assert len(WIND_DIRECTIONS) <= 1 << WIND_BITS


# --- JSON -------------------------------------------------------------------

def encode_json(reading: Reading) -> bytes:
    return json.dumps(reading.to_dict(), separators=(",", ":")).encode("utf-8")


def decode_json(payload: bytes) -> Reading:
    data = json.loads(payload.decode("utf-8"))
    return Reading(
        temperatura=float(data["temperatura"]),
        humedad=int(data["humedad"]),
        direccion_viento=str(data["direccion_viento"]),
    )


# --- Compacto (3 bytes) -----------------------------------------------------

def encode_compact(reading: Reading) -> bytes:
    temp = round(reading.temperatura * TEMP_SCALE)
    wind = WIND_DIRECTIONS.index(reading.direccion_viento)
    packed = (temp << _TEMP_SHIFT) | (reading.humedad << _HUMIDITY_SHIFT) | wind
    return packed.to_bytes(PAYLOAD_SIZE, "big")


def decode_compact(payload: bytes) -> Reading:
    if len(payload) != PAYLOAD_SIZE:
        raise ValueError(f"se esperaban {PAYLOAD_SIZE} bytes, llegaron {len(payload)}")
    packed = int.from_bytes(payload, "big")
    temp = (packed >> _TEMP_SHIFT) & _TEMP_MASK
    humidity = (packed >> _HUMIDITY_SHIFT) & _HUMIDITY_MASK
    wind = packed & _WIND_MASK
    return Reading(
        temperatura=temp / TEMP_SCALE,
        humedad=humidity,
        direccion_viento=WIND_DIRECTIONS[wind],
    )


# --- Seleccion por modo -----------------------------------------------------

ENCODERS = {"json": encode_json, "compact": encode_compact}


def decode_auto(payload: bytes) -> Reading:
    """Decodifica segun el tamano: 3 bytes => compacto, otro => JSON."""
    if len(payload) == PAYLOAD_SIZE:
        return decode_compact(payload)
    return decode_json(payload)
