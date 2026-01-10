from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.core.config import get_settings
from app.core.logging import configure_logging
from app.routers import analytics, health, ingest, mapping, marts, pipeline, questions, rules, studies


def create_app() -> FastAPI:
    settings = get_settings()
    configure_logging()

    app = FastAPI(title=settings.api_title, version=settings.api_version)

    app.add_middleware(
        CORSMiddleware,
        allow_origins=settings.cors_origins,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    app.include_router(health.router)
    app.include_router(studies.router, prefix="/studies", tags=["studies"])
    app.include_router(analytics.router, prefix="/analytics", tags=["analytics"])
    app.include_router(ingest.router, tags=["demo"])
    app.include_router(mapping.router, tags=["mapping"])
    app.include_router(marts.router, tags=["marts"])
    app.include_router(rules.router, tags=["rules"])
    app.include_router(questions.router, tags=["questions"])
    app.include_router(pipeline.router, tags=["pipeline"])

    return app


app = create_app()
