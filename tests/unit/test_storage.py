"""Tests unitarios para el backend de almacenamiento local."""

import pytest

from obbot.lakehouse.s3 import LocalStorageBackend


@pytest.mark.asyncio
async def test_local_storage_crud(tmp_path: pytest.TempPathFactory) -> None:
    backend = LocalStorageBackend(base_dir=str(tmp_path))
    bucket = "test-bucket"
    key = "path/to/test_file.txt"
    payload = b"Prueba de almacenamiento local de alta fidelidad"

    # Verificar no existencia
    assert await backend.exists(bucket, key) is False
    with pytest.raises(FileNotFoundError):
        await backend.get_object(bucket, key)

    # Escribir objeto
    await backend.put_object(bucket, key, payload)
    assert await backend.exists(bucket, key) is True

    # Leer objeto
    data = await backend.get_object(bucket, key)
    assert data == payload

    # Listar objetos con prefijo
    objs = await backend.list_objects(bucket, prefix="path/to")
    assert objs == ["path/to/test_file.txt"]

    objs_all = await backend.list_objects(bucket)
    assert objs_all == ["path/to/test_file.txt"]
