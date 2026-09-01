from __future__ import annotations

import httpx

from app.core.config import get_settings


class PostgresRpcError(RuntimeError):
    pass


class SupabasePostgresRpc:
    """Thin wrapper around Supabase's PostgREST RPC endpoint — httpx direct, no SDK.

    Same style as app.storage.blob.SupabaseStorage. This is services/api's first direct
    Postgres access (everything else in this service only talks to Storage) — kept
    deliberately minimal rather than pulling in a Postgres driver, since a single stats
    RPC is all that's needed today.
    """

    def __init__(self, base_url: str, service_role_key: str):
        self._base = base_url.rstrip("/")
        self._headers = {
            "Authorization": f"Bearer {service_role_key}",
            "apikey": service_role_key,
            "Content-Type": "application/json",
        }
        self._client = httpx.Client(timeout=httpx.Timeout(30.0))

    def call(self, function_name: str, params: dict | None = None) -> object:
        resp = self._client.post(
            f"{self._base}/rest/v1/rpc/{function_name}",
            headers=self._headers,
            json=params or {},
        )
        if resp.status_code >= 400:
            raise PostgresRpcError(f"RPC {function_name} failed ({resp.status_code}): {resp.text[:500]}")
        return resp.json()


_postgres_rpc: SupabasePostgresRpc | None = None


def get_postgres_rpc() -> SupabasePostgresRpc:
    global _postgres_rpc
    if _postgres_rpc is None:
        settings = get_settings()
        _postgres_rpc = SupabasePostgresRpc(settings.supabase_url, settings.supabase_service_role_key)
    return _postgres_rpc
