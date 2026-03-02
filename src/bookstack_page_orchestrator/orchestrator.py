from __future__ import annotations

from dataclasses import dataclass
import logging
import re
from typing import Any

from .bookstack_client import PageClient

ALLOWED_EVENTS = {"page_update", "page_create"}
LINK_PATTERN = re.compile(r"\[[^\]]+\]\(([^)]+)\)")
HEADING_PATTERN = re.compile(r"^#\s+(.+?)\s*$", re.MULTILINE)
logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class OrchestrationResult:
    updated_targets: list[int]
    ignored: bool = False
    reason: str | None = None


@dataclass(frozen=True)
class ConfigRule:
    book_name: str
    chapter_name: str | None
    page_name: str
    template: str


@dataclass(frozen=True)
class RenderedTemplate:
    markdown: str
    source_page_ids: set[int]


@dataclass(frozen=True)
class CompiledTemplate:
    template_parts: list[str]
    source_page_ids: list[int | None]
    unresolved_link_literals: list[str]


@dataclass(frozen=True)
class CompiledRule:
    book_name: str
    chapter_name: str | None
    page_name: str
    compiled_template: CompiledTemplate
    source_id_set: set[int]


def _extract_changed_page_id(payload: dict[str, Any]) -> int | None:
    candidates: list[Any] = []

    for key in ("page_id", "id"):
        if key in payload:
            candidates.append(payload.get(key))

    data = payload.get("data")
    if isinstance(data, dict) and "id" in data:
        candidates.append(data.get("id"))

    related_item = payload.get("related_item")
    if isinstance(related_item, dict) and "id" in related_item:
        candidates.append(related_item.get("id"))

    for value in candidates:
        try:
            return int(value)
        except (TypeError, ValueError):
            continue

    return None


def _parse_heading_target(heading_text: str) -> tuple[str, str | None, str]:
    parts = [part.strip() for part in heading_text.split(".") if part.strip()]
    if len(parts) == 2:
        return parts[0], None, parts[1]
    if len(parts) == 3:
        return parts[0], parts[1], parts[2]

    raise ValueError("Heading must be '<book>.<page>' or '<book>.<chapter>.<page>'")


def parse_config_markdown(markdown: str) -> list[ConfigRule]:
    matches = list(HEADING_PATTERN.finditer(markdown))
    rules: list[ConfigRule] = []

    for index, match in enumerate(matches):
        heading_text = match.group(1).strip()
        body_start = match.end()
        body_end = matches[index + 1].start() if index + 1 < len(matches) else len(markdown)
        template = markdown[body_start:body_end].strip()
        if not template:
            continue

        try:
            book_name, chapter_name, page_name = _parse_heading_target(heading_text)
        except ValueError:
            logger.warning("Ignoring invalid config heading", extra={"heading_text": heading_text})
            continue

        rules.append(
            ConfigRule(
                book_name=book_name,
                chapter_name=chapter_name,
                page_name=page_name,
                template=template,
            )
        )

    return rules


def render_template(template: str, page_client: PageClient) -> RenderedTemplate:
    source_ids: set[int] = set()
    parts: list[str] = []
    cursor = 0

    for match in LINK_PATTERN.finditer(template):
        parts.append(template[cursor:match.start()])
        linked_page = page_client.resolve_link_to_page(match.group(1))
        if linked_page is None:
            parts.append(match.group(0))
            cursor = match.end()
            continue

        page_id = int(linked_page["id"])
        source_ids.add(page_id)
        parts.append(page_client.get_page_markdown(page_id))
        cursor = match.end()

    parts.append(template[cursor:])
    return RenderedTemplate(markdown="".join(parts), source_page_ids=source_ids)


def _compile_template(template: str, page_client: PageClient) -> CompiledTemplate:
    template_parts: list[str] = []
    source_page_ids: list[int | None] = []
    unresolved_link_literals: list[str] = []
    cursor = 0

    for match in LINK_PATTERN.finditer(template):
        template_parts.append(template[cursor:match.start()])
        link_markdown = match.group(0)
        linked_page = page_client.resolve_link_to_page(match.group(1))
        if linked_page is None:
            source_page_ids.append(None)
            unresolved_link_literals.append(link_markdown)
        else:
            source_page_ids.append(int(linked_page["id"]))
            unresolved_link_literals.append("")

        cursor = match.end()

    template_parts.append(template[cursor:])
    return CompiledTemplate(
        template_parts=template_parts,
        source_page_ids=source_page_ids,
        unresolved_link_literals=unresolved_link_literals,
    )


def _render_compiled_template(compiled: CompiledTemplate, page_client: PageClient) -> RenderedTemplate:
    parts: list[str] = [compiled.template_parts[0]]
    resolved_source_ids: set[int] = set()

    for index, source_page_id in enumerate(compiled.source_page_ids):
        if source_page_id is None:
            parts.append(compiled.unresolved_link_literals[index])
        else:
            parts.append(page_client.get_page_markdown(source_page_id))
            resolved_source_ids.add(source_page_id)

        parts.append(compiled.template_parts[index + 1])

    return RenderedTemplate(markdown="".join(parts), source_page_ids=resolved_source_ids)


class PageOrchestrator:
    def __init__(
        self,
        page_client: PageClient,
        config_book_name: str,
        config_page_name: str,
        page_recipes: dict[int, list[int]] | None = None,
    ) -> None:
        self._page_client = page_client
        self._config_book_name = config_book_name
        self._config_page_name = config_page_name
        self._page_recipes = page_recipes or {}
        self._config_page_id: int | None = None
        self._compiled_rules: list[CompiledRule] = []

    def _refresh_config_cache(self) -> None:
        logger.info(
            "Refreshing orchestration config cache",
            extra={"config_book_name": self._config_book_name, "config_page_name": self._config_page_name},
        )
        config_page = self._page_client.get_or_create_config_page(
            self._config_book_name,
            self._config_page_name,
        )
        self._config_page_id = int(config_page["id"])
        rules = parse_config_markdown(str(config_page.get("markdown", "")))

        compiled_rules: list[CompiledRule] = []
        for rule in rules:
            compiled_template = _compile_template(rule.template, self._page_client)
            source_id_set = {page_id for page_id in compiled_template.source_page_ids if page_id is not None}
            compiled_rules.append(
                CompiledRule(
                    book_name=rule.book_name,
                    chapter_name=rule.chapter_name,
                    page_name=rule.page_name,
                    compiled_template=compiled_template,
                    source_id_set=source_id_set,
                )
            )

        self._compiled_rules = compiled_rules
        logger.info(
            "Config cache refreshed",
            extra={
                "config_page_id": self._config_page_id,
                "rule_count": len(self._compiled_rules),
            },
        )

    def _ensure_config_cache(self) -> None:
        if self._config_page_id is None:
            self._refresh_config_cache()

    def _sync_from_config(self, *, changed_page_id: int | None) -> OrchestrationResult:
        self._ensure_config_cache()

        if self._config_page_id is None:
            return OrchestrationResult(updated_targets=[], ignored=True, reason="config_not_loaded")

        if changed_page_id is not None and changed_page_id == self._config_page_id:
            logger.info("Config page was updated, refreshing cache", extra={"config_page_id": self._config_page_id})
            self._refresh_config_cache()

        if not self._compiled_rules:
            return OrchestrationResult(updated_targets=[], ignored=True, reason="no_rules")

        if changed_page_id is None or changed_page_id == self._config_page_id:
            candidate_rules = self._compiled_rules
        else:
            candidate_rules = [
                rule
                for rule in self._compiled_rules
                if changed_page_id in rule.source_id_set
            ]

        if changed_page_id is not None and changed_page_id != self._config_page_id and not candidate_rules:
            return OrchestrationResult(updated_targets=[], ignored=True, reason="not_a_source_page")

        updated_target_ids: list[int] = []

        for rule in candidate_rules:
            rendered = _render_compiled_template(rule.compiled_template, self._page_client)
            target_page = self._page_client.upsert_target_page(
                book_name=rule.book_name,
                chapter_name=rule.chapter_name,
                page_name=rule.page_name,
                markdown=rendered.markdown,
            )
            target_page_id = int(target_page["id"])

            if changed_page_id is not None and changed_page_id == target_page_id:
                logger.debug("Skipping update for loop prevention", extra={"target_page_id": target_page_id})
                continue

            if (
                changed_page_id is not None
                and changed_page_id != self._config_page_id
                and changed_page_id not in rendered.source_page_ids
            ):
                continue

            self._page_client.update_page_markdown(target_page_id, rendered.markdown)
            updated_target_ids.append(target_page_id)

        return OrchestrationResult(updated_targets=updated_target_ids)

    def initialize(self) -> OrchestrationResult:
        logger.info("Initializing orchestration from config page")
        return self._sync_from_config(changed_page_id=None)

    def process_webhook(self, payload: dict[str, Any]) -> OrchestrationResult:
        event = payload.get("event")
        if event not in ALLOWED_EVENTS:
            return OrchestrationResult(updated_targets=[], ignored=True, reason="unsupported_event")

        changed_page_id = _extract_changed_page_id(payload)
        if changed_page_id is None:
            return OrchestrationResult(updated_targets=[], ignored=True, reason="missing_page_id")
        logger.debug("Processing webhook against config page", extra={"event": event, "changed_page_id": changed_page_id})
        return self._sync_from_config(changed_page_id=changed_page_id)
