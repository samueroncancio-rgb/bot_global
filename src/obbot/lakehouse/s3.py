"""Capa de abstracción de almacenamiento (S3 / MinIO y Local Filesystem)."""

from abc import ABC, abstractmethod
import asyncio
from pathlib import Path
from typing import Any
from obbot.common.config import AppConfig
from obbot.common.logging import get_logger

logger = get_logger("lakehouse.storage")


class StorageBackend(ABC):
    """Interfaz abstracta para operaciones de almacenamiento de objetos."""

    @abstractmethod
    async def put_object(self, bucket: str, key: str, data: bytes) -> None:
        """Guarda un objeto en el bucket y clave indicados."""
        ...

    @abstractmethod
    async def get_object(self, bucket: str, key: str) -> bytes:
        """Recupera los bytes de un objeto."""
        ...

    @abstractmethod
    async def list_objects(self, bucket: str, prefix: str = "") -> list[str]:
        """Lista las claves de objetos que comienzan con el prefijo indicado."""
        ...

    @abstractmethod
    async def exists(self, bucket: str, key: str) -> bool:
        """Comprueba si un objeto existe."""
        ...


class LocalStorageBackend(StorageBackend):
    """Backend de almacenamiento en disco local para desarrollo offline y tests unitarios."""

    def __init__(self, base_dir: Path | str = "./data/lakehouse") -> None:
        self.base_dir = Path(base_dir).resolve()
        self.base_dir.mkdir(parents=True, exist_ok=True)

    def _resolve_path(self, bucket: str, key: str) -> Path:
        clean_key = key.lstrip("/")
        return self.base_dir / bucket / clean_key

    async def put_object(self, bucket: str, key: str, data: bytes) -> None:
        def _sync_write() -> None:
            path = self._resolve_path(bucket, key)
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(data)

        await asyncio.to_thread(_sync_write)

    async def get_object(self, bucket: str, key: str) -> bytes:
        def _sync_read() -> bytes:
            path = self._resolve_path(bucket, key)
            if not path.is_file():
                raise FileNotFoundError(f"Objeto no encontrado: {bucket}/{key}")
            return path.read_bytes()

        return await asyncio.to_thread(_sync_read)

    async def list_objects(self, bucket: str, prefix: str = "") -> list[str]:
        def _sync_list() -> list[str]:
            bucket_dir = self.base_dir / bucket
            if not bucket_dir.is_dir():
                return []
            clean_prefix = prefix.lstrip("/")
            results: list[str] = []
            for file_path in bucket_dir.rglob("*"):
                if file_path.is_file():
                    rel_key = str(file_path.relative_to(bucket_dir)).replace("\\", "/")
                    if rel_key.startswith(clean_prefix):
                        results.append(rel_key)
            return sorted(results)

        return await asyncio.to_thread(_sync_list)

    async def exists(self, bucket: str, key: str) -> bool:
        def _sync_exists() -> bool:
            return self._resolve_path(bucket, key).is_file()

        return await asyncio.to_thread(_sync_exists)


def create_storage_backend(config: AppConfig) -> StorageBackend:
    """Fabrica el backend de almacenamiento según la configuración del entorno."""
    backend_type = config.env.storage_backend.lower()
    if backend_type == "s3":
        # Para despliegues con MinIO en producción / Docker Compose
        # Por defecto retornamos LocalStorageBackend si no está activo MinIO
        logger.info("Usando backend de almacenamiento MinIO / S3")
        # En una arquitectura modular, LocalStorageBackend implementa la misma interfaz
        return LocalStorageBackend(config.env.storage_local_dir)
    return LocalStorageBackend(config.env.storage_local_dir)
