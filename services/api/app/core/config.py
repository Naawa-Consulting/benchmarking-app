import os
from dataclasses import dataclass


def _parse_origins(value: str | None) -> list[str]:
    if not value:
        return ["http://localhost:3000"]
    return [origin.strip() for origin in value.split(",") if origin.strip()]


@dataclass
class Settings:
    api_title: str
    api_version: str
    cors_origins: list[str]
    supabase_url: str
    supabase_service_role_key: str
    storage_bucket: str
    internal_api_key: str | None


def get_settings() -> Settings:
    return Settings(
        api_title=os.getenv("API_TITLE", "Benchmarking API"),
        api_version=os.getenv("API_VERSION", "0.1.0"),
        cors_origins=_parse_origins(os.getenv("CORS_ORIGINS")),
        supabase_url=os.getenv("SUPABASE_URL", ""),
        supabase_service_role_key=os.getenv("SUPABASE_SERVICE_ROLE_KEY", ""),
        storage_bucket=os.getenv("BBS_STORAGE_BUCKET", "bbs-pipeline"),
        internal_api_key=os.getenv("INTERNAL_API_KEY") or None,
    )
