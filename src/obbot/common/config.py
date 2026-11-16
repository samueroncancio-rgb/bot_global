"""Gestor de configuración centralizada mediante Pydantic y archivos YAML."""

from pathlib import Path
from typing import Any

import yaml
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class EnvSettings(BaseSettings):
    """Variables de entorno y secretos del sistema."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    bot_mode: str = Field(default="paper")  # replay | paper | testnet | live
    binance_api_key: str = Field(default="")
    binance_api_secret: str = Field(default="")
    binance_testnet: bool = Field(default=False)

    okx_api_key: str = Field(default="")
    okx_api_secret: str = Field(default="")
    okx_passphrase: str = Field(default="")

    bybit_api_key: str = Field(default="")
    bybit_api_secret: str = Field(default="")
    hyperliquid_wallet_address: str = Field(default="")

    bus_type: str = Field(default="memory")  # memory | redpanda
    redpanda_brokers: str = Field(default="localhost:19092")

    storage_backend: str = Field(default="local")  # local | s3
    storage_local_dir: str = Field(default="./data/lakehouse")
    s3_endpoint_url: str = Field(default="http://localhost:9000")
    s3_access_key: str = Field(default="minioadmin")
    s3_secret_key: str = Field(default="minioadmin")
    s3_region: str = Field(default="us-east-1")
    s3_bucket_bronze: str = Field(default="lakehouse-bronze")
    s3_bucket_silver: str = Field(default="lakehouse-silver")
    s3_bucket_gold: str = Field(default="lakehouse-gold")

    metrics_port: int = Field(default=8000)
    health_port: int = Field(default=8080)

    llm_provider: str = Field(default="gemini")
    llm_model: str = Field(default="gemini-2.5-flash")
    gemini_api_key: str = Field(default="")
    llm_base_url: str = Field(default="")
    llm_api_key: str = Field(default="")


def load_yaml(file_path: Path | str) -> dict[str, Any]:
    """Carga un archivo YAML de configuración de forma segura."""
    path = Path(file_path)
    if not path.exists():
        raise FileNotFoundError(f"Archivo de configuración no encontrado: {path}")
    with open(path, encoding="utf-8") as f:
        data = yaml.safe_load(f)
    return data or {}


class AppConfig:
    """Configuración consolidada del sistema (entorno + archivos YAML)."""

    def __init__(self, configs_dir: Path | str = "configs") -> None:
        self.env = EnvSettings()
        base_dir = Path(configs_dir)
        self.exchanges = load_yaml(base_dir / "exchanges.yaml")
        self.symbols = load_yaml(base_dir / "symbols.yaml")
        self.risk = load_yaml(base_dir / "risk.yaml")
        self.features = load_yaml(base_dir / "features.yaml")
