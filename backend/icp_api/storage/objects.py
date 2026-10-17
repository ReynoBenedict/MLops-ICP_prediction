"""Object storage: local filesystem (development), S3-compatible API (Cloudflare R2), or Cloudflare
KV through the project's own Worker (``GatewayObjectStore``; the zero-cost deployment path).

Key layout (one bucket, prefixes; see docs/STORAGE.md):

* ``datasets/{icp|wti}/{sha256}.csv``      immutable, content-addressed dataset versions
* ``exports/forecasts/{YYYY}/{MM}/{id}.csv`` immutable forecast exports (lifecycle: 180 days)
* ``mlflow/...``                            MLflow artifacts (written by MLflow, not this module)
* ``models/production.json``, ``models/bundles/{version}/...``  exported model bundles (icp_ml.bundle)

Objects are write-once: ``put(..., overwrite=False)`` refuses to replace an existing key
(R2 enforces this atomically with ``If-None-Match: *``).
"""

from __future__ import annotations

import os
import re
import secrets
import time
from pathlib import Path
from typing import Any, Protocol

import httpx

from icp_api.storage.sql import StorageError, internal_url

_KEY = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._/-]{0,511}$")


class ObjectNotFound(StorageError):
    pass


class ObjectExists(StorageError):
    pass


def validate_key(key: str) -> str:
    if not _KEY.match(key) or ".." in key or "//" in key or key.endswith("/"):
        raise ValueError(f"Invalid object key: {key!r}")
    return key


class ObjectStore(Protocol):
    backend: str

    def put(self, key: str, data: bytes, content_type: str, overwrite: bool = False) -> None: ...

    def get(self, key: str) -> bytes: ...

    def exists(self, key: str) -> bool: ...

    def ping(self) -> None: ...


class LocalObjectStore:
    backend = "local"

    def __init__(self, root: Path):
        self.root = root
        root.mkdir(parents=True, exist_ok=True)

    def _path(self, key: str) -> Path:
        path = (self.root / validate_key(key)).resolve()
        if self.root.resolve() not in path.parents:
            raise ValueError("object key escapes the store root")
        return path

    def put(self, key: str, data: bytes, content_type: str, overwrite: bool = False) -> None:
        path = self._path(key)
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_name(f".{path.name}.{secrets.token_hex(4)}.tmp")
        tmp.write_bytes(data)
        try:
            if overwrite:
                os.replace(tmp, path)
            else:
                try:
                    os.link(tmp, path)  # atomic create-if-absent
                except FileExistsError as exc:
                    raise ObjectExists(key) from exc
        finally:
            tmp.unlink(missing_ok=True)

    def get(self, key: str) -> bytes:
        try:
            return self._path(key).read_bytes()
        except FileNotFoundError as exc:
            raise ObjectNotFound(key) from exc

    def exists(self, key: str) -> bool:
        return self._path(key).is_file()

    def ping(self) -> None:
        if not os.access(self.root, os.W_OK):
            raise StorageError(f"object store root {self.root} is not writable")


class S3ObjectStore:
    """R2 via its S3-compatible endpoint (``https://<ACCOUNT_ID>.r2.cloudflarestorage.com``)."""

    backend = "s3"

    def __init__(
        self,
        endpoint_url: str,
        bucket: str,
        access_key_id: str,
        secret_access_key: str,
        region: str = "auto",
        client: Any = None,
    ):
        self.bucket = bucket
        if client is None:
            import boto3
            from botocore.config import Config

            client = boto3.client(
                "s3",
                endpoint_url=endpoint_url,
                aws_access_key_id=access_key_id,
                aws_secret_access_key=secret_access_key,
                region_name=region,
                config=Config(
                    connect_timeout=5, read_timeout=30, retries={"max_attempts": 3, "mode": "standard"}
                ),
            )
        self.client = client

    @staticmethod
    def _code(exc: Exception) -> str:
        return str(getattr(exc, "response", {}).get("Error", {}).get("Code", ""))

    def put(self, key: str, data: bytes, content_type: str, overwrite: bool = False) -> None:
        from botocore.exceptions import BotoCoreError, ClientError

        kwargs: dict[str, Any] = {
            "Bucket": self.bucket,
            "Key": validate_key(key),
            "Body": data,
            "ContentType": content_type,
        }
        if not overwrite:
            kwargs["IfNoneMatch"] = "*"
        try:
            self.client.put_object(**kwargs)
        except ClientError as exc:
            if self._code(exc) in ("PreconditionFailed", "412", "ConditionalRequestConflict"):
                raise ObjectExists(key) from exc
            raise StorageError(f"object store put failed ({self._code(exc)})") from exc
        except BotoCoreError as exc:
            raise StorageError(f"object store unavailable: {type(exc).__name__}") from exc

    def get(self, key: str) -> bytes:
        from botocore.exceptions import BotoCoreError, ClientError

        try:
            return bytes(self.client.get_object(Bucket=self.bucket, Key=validate_key(key))["Body"].read())
        except ClientError as exc:
            if self._code(exc) in ("NoSuchKey", "404"):
                raise ObjectNotFound(key) from exc
            raise StorageError(f"object store get failed ({self._code(exc)})") from exc
        except BotoCoreError as exc:
            raise StorageError(f"object store unavailable: {type(exc).__name__}") from exc

    def exists(self, key: str) -> bool:
        from botocore.exceptions import ClientError

        try:
            self.client.head_object(Bucket=self.bucket, Key=validate_key(key))
            return True
        except ClientError as exc:
            if self._code(exc) in ("404", "NoSuchKey", "NotFound"):
                return False
            raise StorageError(f"object store head failed ({self._code(exc)})") from exc

    def ping(self) -> None:
        from botocore.exceptions import BotoCoreError, ClientError

        try:
            self.client.head_bucket(Bucket=self.bucket)
        except (ClientError, BotoCoreError) as exc:
            raise StorageError(f"object store bucket check failed: {type(exc).__name__}") from exc


class GatewayObjectStore:
    """Objects in Cloudflare KV, reached through the Worker's internal data plane.

    ``PUT/GET/HEAD <gateway>/api/internal/objects/<key>`` authenticated with the gateway shared
    secret. Write-once is requested with ``If-None-Match: *`` (the Worker answers 412 when the key
    exists). KV is eventually consistent (seconds, up to ~60 s across regions), so the check is
    best-effort; every write-once key in this project is content-addressed or carries a unique id,
    which makes a lost race harmless (identical content). Values are limited to 25 MiB by KV.
    """

    backend = "gateway"
    _RETRY_STATUS = {429, 502, 503, 504}

    def __init__(
        self,
        gateway_url: str,
        shared_secret: str,
        timeout: float = 30.0,
        max_attempts: int = 3,
        transport: httpx.BaseTransport | None = None,
    ):
        if not shared_secret:
            raise ValueError("GatewayObjectStore requires the gateway shared secret")
        self.gateway_url = gateway_url
        self._client = httpx.Client(
            timeout=timeout, headers={"x-gateway-secret": shared_secret}, transport=transport
        )
        self.max_attempts = max_attempts

    def _url(self, key: str) -> str:
        return internal_url(self.gateway_url, f"objects/{validate_key(key)}")

    def _request(self, method: str, url: str, idempotent: bool, **kw: Any) -> httpx.Response:
        last: Exception | None = None
        for attempt in range(1, self.max_attempts + 1):
            try:
                resp = self._client.request(method, url, **kw)
            except httpx.ConnectError as exc:
                last = exc
            except httpx.TimeoutException as exc:
                if not idempotent:
                    raise StorageError("object store request timed out") from exc
                last = exc
            else:
                if resp.status_code in self._RETRY_STATUS:
                    last = StorageError(f"object store HTTP {resp.status_code}")
                elif resp.status_code in (401, 403):
                    raise StorageError("object store rejected the gateway secret")
                else:
                    return resp
            if attempt < self.max_attempts:
                time.sleep(0.25 * 2 ** (attempt - 1))
        raise StorageError(f"object store unavailable after {self.max_attempts} attempts: {last}")

    def put(self, key: str, data: bytes, content_type: str, overwrite: bool = False) -> None:
        headers = {"content-type": content_type}
        if not overwrite:
            headers["if-none-match"] = "*"
        resp = self._request("PUT", self._url(key), idempotent=False, content=data, headers=headers)
        if resp.status_code == 412:
            raise ObjectExists(key)
        if resp.status_code >= 400:
            raise StorageError(f"object store put failed (HTTP {resp.status_code})")

    def get(self, key: str) -> bytes:
        resp = self._request("GET", self._url(key), idempotent=True)
        if resp.status_code == 404:
            raise ObjectNotFound(key)
        if resp.status_code >= 400:
            raise StorageError(f"object store get failed (HTTP {resp.status_code})")
        return bytes(resp.content)

    def exists(self, key: str) -> bool:
        resp = self._request("HEAD", self._url(key), idempotent=True)
        if resp.status_code == 404:
            return False
        if resp.status_code >= 400:
            raise StorageError(f"object store head failed (HTTP {resp.status_code})")
        return True

    def ping(self) -> None:
        resp = self._request("GET", internal_url(self.gateway_url, "health"), idempotent=True)
        if resp.status_code >= 400:
            raise StorageError(f"gateway data plane unavailable (HTTP {resp.status_code})")
        try:
            payload = resp.json()
        except ValueError as exc:
            raise StorageError("gateway data plane returned a non-JSON health response") from exc
        if not payload.get("kv"):
            raise StorageError("gateway has no KV binding (OBJECTS) configured")
