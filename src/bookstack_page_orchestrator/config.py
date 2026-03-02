from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    bookstack_url: str
    bookstack_token_id: str
    bookstack_token_secret: str
    config_book_name: str
    config_page_name: str
    page_recipes: dict[int, list[int]]
    log_level: str


def _normalize_log_level(raw_level: str | None) -> str:
    if raw_level is None:
        return "DEBUG"

    level = raw_level.strip().upper()
    if level == "VERBOSE":
        return "DEBUG"

    if level not in logging.getLevelNamesMapping():
        raise ValueError(f"Invalid LOG_LEVEL value: {raw_level!r}")

    return level


def _normalize_page_recipes(raw: str) -> dict[int, list[int]]:
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ValueError("PAGE_RECIPES must be valid JSON") from exc

    if not isinstance(parsed, dict):
        raise ValueError("PAGE_RECIPES must be a JSON object mapping target ids to source id lists")

    recipes: dict[int, list[int]] = {}
    for target_id, sources in parsed.items():
        try:
            normalized_target = int(target_id)
        except (TypeError, ValueError) as exc:
            raise ValueError(f"Invalid target page id in PAGE_RECIPES: {target_id!r}") from exc

        if not isinstance(sources, list):
            raise ValueError(
                f"Invalid sources for target {target_id!r}: expected list of source ids"
            )

        normalized_sources: list[int] = []
        for source_id in sources:
            try:
                normalized_sources.append(int(source_id))
            except (TypeError, ValueError) as exc:
                raise ValueError(
                    f"Invalid source page id {source_id!r} for target {target_id!r}"
                ) from exc

        recipes[normalized_target] = normalized_sources

    return recipes


def load_settings() -> Settings:
    url = os.getenv("BOOKSTACK_URL")
    token_id = os.getenv("BOOKSTACK_TOKEN_ID")
    token_secret = os.getenv("BOOKSTACK_TOKEN_SECRET")
    config_book_name = os.getenv("CONFIG_BOOK_NAME")
    config_page_name = os.getenv("CONFIG_PAGE_NAME")
    raw_recipes = os.getenv("PAGE_RECIPES")
    log_level = os.getenv("LOG_LEVEL")

    missing = []
    if not url:
        missing.append("BOOKSTACK_URL")
    if not token_id:
        missing.append("BOOKSTACK_TOKEN_ID")
    if not token_secret:
        missing.append("BOOKSTACK_TOKEN_SECRET")
    if not config_book_name:
        missing.append("CONFIG_BOOK_NAME")
    if not config_page_name:
        missing.append("CONFIG_PAGE_NAME")

    if missing:
        missing_csv = ", ".join(missing)
        raise ValueError(f"Missing required environment variables: {missing_csv}")

    return Settings(
        bookstack_url=url,
        bookstack_token_id=token_id,
        bookstack_token_secret=token_secret,
        config_book_name=config_book_name,
        config_page_name=config_page_name,
        page_recipes=_normalize_page_recipes(raw_recipes) if raw_recipes else {},
        log_level=_normalize_log_level(log_level),
    )
