from __future__ import annotations

import logging
import time
from typing import Any

from fastapi import BackgroundTasks, FastAPI, HTTPException

from .bookstack_client import BookstackPageClient
from .config import load_settings
from .orchestrator import PageOrchestrator

logger = logging.getLogger(__name__)


def configure_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
        force=True,
    )


def _process_webhook_in_background(
    orchestrator: PageOrchestrator,
    payload: dict[str, Any],
    response_returned_at: float,
) -> None:
    processing_started_at = time.perf_counter()
    logger.debug("Background webhook processing started")

    try:
        result = orchestrator.process_webhook(payload)
        total_since_response = time.perf_counter() - response_returned_at
        processing_duration = time.perf_counter() - processing_started_at
        logger.info(
            "Background webhook processing finished",
            extra={
                "ignored": result.ignored,
                "reason": result.reason,
                "updated_targets": result.updated_targets,
                "seconds_from_response_to_finish": total_since_response,
                "processing_seconds": processing_duration,
            },
        )
    except Exception:
        total_since_response = time.perf_counter() - response_returned_at
        logger.exception(
            "Background webhook processing failed",
            extra={"seconds_from_response_to_failure": total_since_response},
        )


def create_app() -> FastAPI:
    settings = load_settings()
    configure_logging(settings.log_level)
    logger.info("Starting Bookstack Page Orchestrator")
    logger.debug("Log level configured")

    page_client = BookstackPageClient(
        base_url=settings.bookstack_url,
        token_id=settings.bookstack_token_id,
        token_secret=settings.bookstack_token_secret,
    )
    orchestrator = PageOrchestrator(
        page_client=page_client,
        config_book_name=settings.config_book_name,
        config_page_name=settings.config_page_name,
        page_recipes=settings.page_recipes,
    )

    try:
        init_result = orchestrator.initialize()
        logger.info(
            "Startup config sync completed",
            extra={"updated_targets": init_result.updated_targets, "ignored": init_result.ignored},
        )
    except Exception:
        logger.exception("Startup config sync failed")

    app = FastAPI(title="Bookstack Page Orchestrator")
    app.state.orchestrator = orchestrator

    @app.post("/webhook")
    def webhook(payload: dict[str, Any], background_tasks: BackgroundTasks) -> dict[str, Any]:
        logger.debug("Received webhook payload")
        response_returned_at = time.perf_counter()
        background_tasks.add_task(
            _process_webhook_in_background,
            app.state.orchestrator,
            payload,
            response_returned_at,
        )
        return {"accepted": True}

    @app.get("/health")
    def health() -> dict[str, str]:
        return {"status": "ok"}

    return app


try:
    app = create_app()
except ValueError as exc:
    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
        force=True,
    )
    logger.exception("Application configuration failed during startup")
    app = FastAPI(title="Bookstack Page Orchestrator")

    @app.post("/webhook")
    def webhook_unavailable() -> dict[str, str]:
        raise HTTPException(status_code=500, detail=str(exc))

    @app.get("/health")
    def health_unavailable() -> dict[str, str]:
        return {"status": "misconfigured"}
