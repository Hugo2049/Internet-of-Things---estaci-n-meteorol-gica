"""Consumidor: se suscribe al topic y grafica la telemetria en vivo.

Decodifica automaticamente JSON o el formato compacto de 3 bytes.

Uso:
    python -m weather_station.consumer
    python -m weather_station.consumer --save output/grafica.png
"""

from __future__ import annotations

import argparse
import logging
from datetime import datetime
from pathlib import Path

import matplotlib.dates as mdates
import matplotlib.pyplot as plt
from confluent_kafka import Consumer, KafkaError, KafkaException

from .codec import decode_auto
from .config import KafkaSettings
from .sensor import HUMIDITY_MAX, TEMP_MAX, WIND_DIRECTIONS, Reading

log = logging.getLogger("consumer")


class LivePlot:
    """Tres graficas (temperatura, humedad, viento) que se redibujan por mensaje."""

    def __init__(self, topic: str) -> None:
        self.times: list[datetime] = []
        self.temps: list[float] = []
        self.hums: list[int] = []
        self.winds: list[int] = []

        plt.ion()
        self.fig, (self.ax_t, self.ax_h, self.ax_w) = plt.subplots(
            3, 1, sharex=True, figsize=(11, 8), constrained_layout=True,
        )
        self.fig.suptitle(f"Estacion meteorologica - topic {topic}")
        self.fig.canvas.manager.set_window_title("Estacion meteorologica IoT")
        (self.line_t,) = self.ax_t.plot([], [], "o-", color="tab:red")
        (self.line_h,) = self.ax_h.plot([], [], "o-", color="tab:blue")
        (self.line_w,) = self.ax_w.plot([], [], "o", color="tab:green")

        self.ax_t.set_ylabel("Temperatura (°C)")
        self.ax_t.set_ylim(0, TEMP_MAX)
        self.ax_h.set_ylabel("Humedad relativa (%)")
        self.ax_h.set_ylim(0, HUMIDITY_MAX)
        self.ax_w.set_ylabel("Direccion viento")
        self.ax_w.set_yticks(range(len(WIND_DIRECTIONS)), WIND_DIRECTIONS)
        self.ax_w.set_ylim(-0.5, len(WIND_DIRECTIONS) - 0.5)
        self.ax_w.set_xlabel("Hora de recepcion")
        self.ax_w.xaxis_date()
        self.ax_w.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M:%S"))
        for ax in (self.ax_t, self.ax_h, self.ax_w):
            ax.grid(alpha=0.3)
        self.status = self.fig.text(0.01, 0.005, "Esperando datos...", fontsize=9)

    def add(self, ts: datetime, reading: Reading) -> None:
        self.times.append(ts)
        self.temps.append(reading.temperatura)
        self.hums.append(reading.humedad)
        self.winds.append(WIND_DIRECTIONS.index(reading.direccion_viento))

        self.line_t.set_data(self.times, self.temps)
        self.line_h.set_data(self.times, self.hums)
        self.line_w.set_data(self.times, self.winds)
        for ax in (self.ax_t, self.ax_h, self.ax_w):
            ax.relim()
            ax.autoscale_view(scalex=True, scaley=False)
        self.fig.autofmt_xdate()
        self.status.set_text(
            f"Mensajes: {len(self.times)} | Ultimo: {reading.temperatura:.2f} °C, "
            f"{reading.humedad} %, {reading.direccion_viento}"
        )
        self.fig.canvas.draw_idle()

    def is_open(self) -> bool:
        return plt.fignum_exists(self.fig.number)

    def save(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        self.fig.savefig(path, dpi=150)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Consumidor y graficador de la estacion")
    p.add_argument("--group-id", default=None,
                   help="consumer group (default: <topic>-dashboard)")
    p.add_argument("--offset-reset", choices=("earliest", "latest"), default="earliest",
                   help="desde donde leer si el grupo no tiene offset guardado")
    p.add_argument("--save", type=Path, default=None,
                   help="guardar la grafica en este PNG en cada actualizacion")
    return p.parse_args()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    args = parse_args()
    settings = KafkaSettings.from_env()

    consumer = Consumer({
        **settings.client_config(),
        "group.id": args.group_id or f"{settings.topic}-dashboard",
        "auto.offset.reset": args.offset_reset,
        "enable.auto.commit": True,
    })
    consumer.subscribe([settings.topic])
    plot = LivePlot(settings.topic)
    log.info("Suscrito a '%s'. Cierre la ventana o Ctrl+C para salir.", settings.topic)

    try:
        while plot.is_open():
            msg = consumer.poll(0.2)
            if msg is not None:
                if msg.error():
                    if msg.error().code() != KafkaError._PARTITION_EOF:
                        raise KafkaException(msg.error())
                else:
                    try:
                        reading = decode_auto(msg.value())
                    except (ValueError, KeyError) as exc:
                        log.warning("Mensaje invalido en offset %d: %s", msg.offset(), exc)
                    else:
                        _, ts_ms = msg.timestamp()
                        ts = datetime.fromtimestamp(ts_ms / 1000) if ts_ms > 0 else datetime.now()
                        log.info("offset %d key=%s %d bytes -> %s", msg.offset(),
                                 msg.key(), len(msg.value()), reading.to_dict())
                        plot.add(ts, reading)
                        if args.save:
                            plot.save(args.save)
            plt.pause(0.05)
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()
        log.info("Consumidor cerrado.")


if __name__ == "__main__":
    main()
