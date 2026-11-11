"""Configuracion de Kafka leida desde variables de entorno (.env)."""

from __future__ import annotations

import os
from dataclasses import dataclass

from dotenv import load_dotenv

load_dotenv()


class ConfigError(RuntimeError):
    """Falta una variable de configuracion obligatoria."""


def _require(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise ConfigError(
            f"Falta la variable de entorno {name}. "
            "Copie .env.example a .env y completela."
        )
    return value


@dataclass(frozen=True)
class KafkaSettings:
    bootstrap_servers: str
    security_protocol: str
    sasl_mechanism: str
    username: str
    password: str
    topic: str

    @classmethod
    def from_env(cls) -> "KafkaSettings":
        return cls(
            bootstrap_servers=_require("KAFKA_BOOTSTRAP_SERVERS"),
            security_protocol=os.getenv("KAFKA_SECURITY_PROTOCOL", "SASL_PLAINTEXT"),
            sasl_mechanism=os.getenv("KAFKA_SASL_MECHANISM", "PLAIN"),
            username=_require("KAFKA_USERNAME"),
            password=_require("KAFKA_PASSWORD"),
            topic=_require("KAFKA_TOPIC"),
        )

    def client_config(self) -> dict[str, str]:
        """Parametros comunes para Producer y Consumer de confluent-kafka."""
        return {
            "bootstrap.servers": self.bootstrap_servers,
            "security.protocol": self.security_protocol,
            "sasl.mechanisms": self.sasl_mechanism,
            "sasl.username": self.username,
            "sasl.password": self.password,
        }
