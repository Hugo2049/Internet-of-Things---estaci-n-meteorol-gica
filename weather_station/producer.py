"""Dispositivo IoT: publica lecturas al broker Kafka cada 15-30 s hasta Ctrl+C.

Uso:
    python -m weather_station.producer                 # JSON (fase 3.2)
    python -m weather_station.producer --mode compact  # 3 bytes (fase 3.4)
"""

from __future__ import annotations

import argparse
import logging
import random
import signal
import threading

from confluent_kafka import KafkaError, Message, Producer

from .codec import ENCODERS
from .config import KafkaSettings
from .sensor import WeatherSensor

log = logging.getLogger("producer")


def _on_delivery(err: KafkaError | None, msg: Message) -> None:
    if err is not None:
        log.error("Fallo la entrega: %s", err)
    else:
        log.info(
            "Entregado a %s [particion %d, offset %d] (%d bytes)",
            msg.topic(), msg.partition(), msg.offset(), len(msg.value()),
        )


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Productor de la estacion meteorologica")
    p.add_argument("--mode", choices=sorted(ENCODERS), default="json",
                   help="formato del payload (default: json)")
    p.add_argument("--key", default="sensor1", help="key del mensaje (id del sensor)")
    p.add_argument("--min-interval", type=float, default=15.0, help="segundos (default 15)")
    p.add_argument("--max-interval", type=float, default=30.0, help="segundos (default 30)")
    p.add_argument("--count", type=int, default=0,
                   help="cantidad de mensajes a enviar (0 = infinito)")
    p.add_argument("--seed", type=int, default=None, help="semilla del generador")
    args = p.parse_args()
    if not 0 <= args.min_interval <= args.max_interval:
        p.error("se requiere 0 <= --min-interval <= --max-interval")
    return args


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    args = parse_args()
    settings = KafkaSettings.from_env()

    producer = Producer({
        **settings.client_config(),
        "client.id": f"weather-station-{args.key}",
        "acks": "all",
        "enable.idempotence": True,
    })
    sensor = WeatherSensor(seed=args.seed)
    encode = ENCODERS[args.mode]
    rng = random.Random(args.seed)

    stop = threading.Event()
    signal.signal(signal.SIGINT, lambda *_: stop.set())
    signal.signal(signal.SIGTERM, lambda *_: stop.set())

    log.info("Publicando en topic '%s' (modo %s). Ctrl+C para detener.", settings.topic, args.mode)
    sent = 0
    try:
        while not stop.is_set():
            reading = sensor.read()
            payload = encode(reading)
            log.info("Lectura %s -> payload %r", reading.to_dict(), payload)
            producer.produce(settings.topic, key=args.key, value=payload, on_delivery=_on_delivery)
            producer.poll(0)
            sent += 1
            if args.count and sent >= args.count:
                break
            stop.wait(rng.uniform(args.min_interval, args.max_interval))
    finally:
        log.info("Deteniendo: enviando mensajes pendientes...")
        remaining = producer.flush(10)
        if remaining:
            log.warning("%d mensajes no se pudieron entregar", remaining)
        log.info("Total enviados: %d", sent)


if __name__ == "__main__":
    main()
