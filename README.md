# Internet of Things — Estación meteorológica

Simulación de un nodo de sensores IoT (temperatura, humedad relativa y dirección
del viento) que publica telemetría en un broker **Apache Kafka** en el borde
(Edge), y un consumidor que la grafica en tiempo real.

## Estructura

```
weather_station/
  config.py     # Configuración de Kafka desde variables de entorno (.env)
  sensor.py     # Generación de lecturas (normal, media 55) y validación de rangos
  codec.py      # Encode/decode JSON y compacto de 3 bytes (24 bits)
  producer.py   # Dispositivo IoT: publica cada 15-30 s hasta Ctrl+C
  consumer.py   # Suscriptor: grafica la telemetría en vivo (matplotlib)
tests/          # Pruebas unitarias (pytest)
```

## Instalación

```bash
py -3.12 -m venv .venv
.venv\Scripts\activate            # Windows  (Linux/macOS: source .venv/bin/activate)
pip install -r requirements-dev.txt
copy .env.example .env            # y completar KAFKA_PASSWORD y KAFKA_TOPIC
```

## Uso

En dos terminales distintas:

```bash
# Terminal 1 — productor (JSON)
python -m weather_station.producer

# Terminal 2 — consumidor con gráfica en vivo
python -m weather_station.consumer --save output/grafica.png
```

Payload restringido a 3 bytes:

```bash
python -m weather_station.producer --mode compact
```

El consumidor detecta automáticamente el formato (3 bytes → compacto, otro → JSON).

Opciones útiles: `--min-interval/--max-interval` (segundos entre envíos),
`--count N` (enviar N mensajes y terminar), `--seed`; en el consumidor
`--offset-reset latest` y `--group-id`.

## Formato compacto (24 bits)

| Bits  | Campo                | Rango          | Codificación          |
|-------|----------------------|----------------|-----------------------|
| 23–10 | Temperatura (14 b)   | 0.00 – 110.00  | `round(t * 100)` → 0–11000 (< 2¹⁴ = 16384) |
| 9–3   | Humedad (7 b)        | 0 – 100        | entero directo (< 2⁷ = 128) |
| 2–0   | Dirección viento (3 b) | N, NO, O, SO, S, SE, E, NE | índice 0–7 |

## Pruebas

```bash
python -m pytest
```
