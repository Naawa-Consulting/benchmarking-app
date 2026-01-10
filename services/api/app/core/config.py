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


def get_settings() -> Settings:
    return Settings(
        api_title=os.getenv("API_TITLE", "Benchmarking API"),
        api_version=os.getenv("API_VERSION", "0.1.0"),
        cors_origins=_parse_origins(os.getenv("CORS_ORIGINS")),
    )
