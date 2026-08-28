from __future__ import annotations

import httpx

from app.core.config import get_settings


class StorageNotFoundError(FileNotFoundError):
    pass


def _is_not_found_body(resp: httpx.Response) -> bool:
    # Supabase Storage's object-get endpoint returns HTTP 400 (not 404) for a missing
    # key, with the real "not found" signal buried in the JSON body's statusCode field
    # (e.g. {"statusCode":"404","error":"Bucket not found","code":"NoSuchBucket"}) —
    # that error/code text is misleading (the bucket exists), Supabase just reuses the
    # same shape for "object not found" too, so we only trust the statusCode field.
    if resp.status_code != 400:
        return False
    try:
        payload = resp.json()
    except ValueError:
        return False
    return isinstance(payload, dict) and str(payload.get("statusCode")) == "404"


class SupabaseStorage:
    """Thin wrapper around the Supabase Storage REST API — httpx direct, no SDK.

    Ported from the aion project, which already runs this pattern in production on
    Render's free tier (no persistent disk): every "file" the pipeline used to keep
    on local disk is read/written as bytes against a private Supabase Storage bucket
    instead, so a container restart never loses data.
    """

    def __init__(self, base_url: str, service_role_key: str, bucket: str):
        self._base = base_url.rstrip("/")
        self._bucket = bucket
        self._headers = {
            "Authorization": f"Bearer {service_role_key}",
            "apikey": service_role_key,
        }

    def _object_url(self, key: str) -> str:
        return f"{self._base}/storage/v1/object/{self._bucket}/{key}"

    def read_bytes(self, key: str) -> bytes:
        resp = httpx.get(self._object_url(key), headers=self._headers, timeout=60.0)
        if resp.status_code == 404 or _is_not_found_body(resp):
            raise StorageNotFoundError(key)
        resp.raise_for_status()
        return resp.content

    def write_bytes(self, key: str, data: bytes, content_type: str = "application/octet-stream") -> None:
        headers = {**self._headers, "Content-Type": content_type, "x-upsert": "true"}
        resp = httpx.post(self._object_url(key), headers=headers, content=data, timeout=120.0)
        if resp.status_code >= 400:
            raise RuntimeError(
                f"Storage write failed ({resp.status_code}) for key={key!r} size={len(data)}: {resp.text}"
            )

    def delete(self, key: str) -> None:
        resp = httpx.delete(self._object_url(key), headers=self._headers, timeout=30.0)
        if resp.status_code not in (200, 204, 404):
            resp.raise_for_status()

    def stat(self, key: str) -> dict | None:
        parent, _, name = key.rpartition("/")
        resp = httpx.post(
            f"{self._base}/storage/v1/object/list/{self._bucket}",
            headers={**self._headers, "Content-Type": "application/json"},
            json={"prefix": parent, "search": name},
            timeout=30.0,
        )
        resp.raise_for_status()
        for item in resp.json():
            if item.get("name") == name:
                return item
        return None

    def exists(self, key: str) -> bool:
        return self.stat(key) is not None

    def list_names(self, prefix: str, limit: int = 1000) -> list[str]:
        """Immediate child names under `prefix` (folders and files), like a non-recursive `ls`.

        Supabase Storage's list endpoint returns names relative to `prefix`, not full
        keys — callers that need full keys should join them back with the prefix.
        """
        normalized_prefix = prefix.rstrip("/")
        resp = httpx.post(
            f"{self._base}/storage/v1/object/list/{self._bucket}",
            headers={**self._headers, "Content-Type": "application/json"},
            json={"prefix": normalized_prefix, "limit": limit},
            timeout=30.0,
        )
        resp.raise_for_status()
        return [item["name"] for item in resp.json() if item.get("name")]

    def move(self, src_key: str, dst_key: str) -> None:
        resp = httpx.post(
            f"{self._base}/storage/v1/object/move",
            headers={**self._headers, "Content-Type": "application/json"},
            json={"bucketId": self._bucket, "sourceKey": src_key, "destinationKey": dst_key},
            timeout=60.0,
        )
        resp.raise_for_status()


_storage: SupabaseStorage | None = None


def get_storage() -> SupabaseStorage:
    global _storage
    if _storage is None:
        settings = get_settings()
        _storage = SupabaseStorage(
            settings.supabase_url, settings.supabase_service_role_key, settings.storage_bucket
        )
    return _storage
