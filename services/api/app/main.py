import logging

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from app.core.config import get_settings
from app.core.logging import configure_logging
from app.routers import (
    analytics,
    demographics,
    filters,
    health,
    ingest,
    mapping,
    marts,
    network,
    pipeline,
    questions,
    question_map,
    rules,
    studies,
    study_config,
    taxonomy,
)


def create_app() -> FastAPI:
    settings = get_settings()
    configure_logging()
    logger = logging.getLogger(__name__)

    app = FastAPI(title=settings.api_title, version=settings.api_version)

    app.add_middleware(
        CORSMiddleware,
        allow_origins=settings.cors_origins,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    if settings.internal_api_key:
        @app.middleware("http")
        async def require_internal_api_key(request: Request, call_next):
            if request.url.path == "/health" or request.method == "OPTIONS":
                return await call_next(request)
            provided = request.headers.get("x-internal-api-key")
            if provided != settings.internal_api_key:
                return JSONResponse(status_code=401, content={"detail": "Unauthorized"})
            return await call_next(request)
    else:
        logger.warning(
            "INTERNAL_API_KEY is not set — this API is reachable without authentication. "
            "Set it before exposing this service publicly."
        )

    app.include_router(health.router)
    app.include_router(studies.router, prefix="/studies", tags=["studies"])
    app.include_router(analytics.router, prefix="/analytics", tags=["analytics"])
    app.include_router(ingest.router, tags=["ingest"])
    app.include_router(mapping.router, tags=["mapping"])
    app.include_router(marts.router, tags=["marts"])
    app.include_router(network.router, tags=["network"])
    app.include_router(rules.router, tags=["rules"])
    app.include_router(questions.router, tags=["questions"])
    app.include_router(question_map.router, tags=["question-map"])
    app.include_router(pipeline.router, tags=["pipeline"])
    app.include_router(demographics.router, tags=["demographics"])
    app.include_router(filters.router, tags=["filters"])
    app.include_router(taxonomy.router, tags=["taxonomy"])
    app.include_router(study_config.router, tags=["study-config"])

    return app


app = create_app()
