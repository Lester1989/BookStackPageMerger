from __future__ import annotations

import logging
import random
import re
from typing import Protocol
from urllib.parse import urlparse

from bookstack import BookStack

CONFIG_SHELF_NAME = "Orchestration Config"
logger = logging.getLogger(__name__)


class PageClient(Protocol):
    def get_page_markdown(self, page_id: int) -> str:
        ...

    def get_or_create_config_page(self, config_book_name: str, config_page_name: str) -> dict:
        ...

    def resolve_link_to_page(self, link: str) -> dict | None:
        ...

    def upsert_target_page(
        self,
        *,
        book_name: str,
        chapter_name: str | None,
        page_name: str,
        markdown: str,
    ) -> dict:
        ...

    def update_page_markdown(self, page_id: int, markdown: str) -> None:
        ...


class BookstackPageClient:
    def __init__(self, base_url: str, token_id: str, token_secret: str) -> None:
        self._bookstack = BookStack(base_url, token_id, token_secret)

    def _get_page(self, page_id: int) -> dict:
        response = self._bookstack._session.request("GET", f"/api/pages/{page_id}")
        response.raise_for_status()
        payload = response.json()
        if not isinstance(payload, dict):
            raise RuntimeError("Unexpected page payload from bookstack API")
        return payload

    def _request(self, method: str, path: str, *, params: dict | None = None, json: dict | None = None):
        logger.debug(
            "BookStack API request",
            extra={"method": method, "path": path, "params": params, "has_json": json is not None},
        )
        response = self._bookstack._session.request(method, path, params=params, json=json)
        try:
            response.raise_for_status()
        except Exception:
            logger.exception(
                "BookStack API request failed",
                extra={
                    "method": method,
                    "path": path,
                    "status_code": response.status_code,
                    "response_text": response.text[:500],
                },
            )
            raise
        return response.json()

    def _list_all(self, path: str, *, count: int = 100) -> list[dict]:
        offset = 0
        items: list[dict] = []
        while True:
            payload = self._request("GET", path, params={"count": count, "offset": offset})
            if not isinstance(payload, dict):
                break
            batch = payload.get("data", [])
            if not isinstance(batch, list):
                break
            items.extend(x for x in batch if isinstance(x, dict))
            if len(batch) < count:
                break
            offset += count
        logger.debug("Listed BookStack entities", extra={"path": path, "count": len(items)})
        return items

    def _find_page_by_name(self, page_name: str) -> dict | None:
        page_name_cf = page_name.casefold()
        for page in self._list_all("/api/pages"):
            if str(page.get("name", "")).casefold() == page_name_cf:
                return self._get_page(int(page["id"]))
        return None

    def _ensure_book(self, book_name: str) -> dict:
        logger.debug("Ensuring book exists", extra={"book_name": book_name})
        book = self._find_book_by_name(book_name)
        if book is None:
            logger.info("Book not found, creating", extra={"book_name": book_name})
            book = self._create_book(book_name)
            logger.info("Book created", extra={"book_name": book_name, "book_id": book.get("id")})
        else:
            logger.debug("Book already exists", extra={"book_name": book_name, "book_id": book.get("id")})
        return book

    def _find_shelf_by_name(self, shelf_name: str) -> dict | None:
        shelf_name_cf = shelf_name.casefold()
        for shelf in self._list_all("/api/shelves"):
            if str(shelf.get("name", "")).casefold() == shelf_name_cf:
                return shelf
        return None

    def _create_shelf(self, shelf_name: str) -> dict:
        payload = self._request("POST", "/api/shelves", json={"name": shelf_name, "description": ""})
        if not isinstance(payload, dict):
            raise RuntimeError("Unexpected create shelf response")
        return payload

    def _ensure_shelf(self, shelf_name: str) -> dict:
        logger.debug("Ensuring shelf exists", extra={"shelf_name": shelf_name})
        shelf = self._find_shelf_by_name(shelf_name)
        if shelf is None:
            logger.info("Shelf not found, creating", extra={"shelf_name": shelf_name})
            shelf = self._create_shelf(shelf_name)
            logger.info("Shelf created", extra={"shelf_name": shelf_name, "shelf_id": shelf.get("id")})
        else:
            logger.debug(
                "Shelf already exists",
                extra={"shelf_name": shelf_name, "shelf_id": shelf.get("id")},
            )
        return shelf

    def _extract_book_ids_from_shelf(self, shelf_payload: dict) -> list[int]:
        books = shelf_payload.get("books", [])
        if not isinstance(books, list):
            return []
        result: list[int] = []
        for book in books:
            if not isinstance(book, dict):
                continue
            try:
                result.append(int(book["id"]))
            except (KeyError, TypeError, ValueError):
                continue
        return result

    def _ensure_book_in_shelf(self, *, shelf_id: int, shelf_name: str, book_id: int) -> None:
        logger.debug(
            "Ensuring book belongs to shelf",
            extra={"shelf_id": shelf_id, "shelf_name": shelf_name, "book_id": book_id},
        )
        shelf_detail = self._request("GET", f"/api/shelves/{shelf_id}")
        if not isinstance(shelf_detail, dict):
            raise RuntimeError("Unexpected shelf payload from bookstack API")

        existing_book_ids = self._extract_book_ids_from_shelf(shelf_detail)
        if book_id in existing_book_ids:
            logger.debug("Book already linked to shelf", extra={"shelf_id": shelf_id, "book_id": book_id})
            return

        merged_ids = sorted(set(existing_book_ids + [book_id]))

        try:
            self._request(
                "PUT",
                f"/api/shelves/{shelf_id}",
                json={
                    "name": shelf_detail.get("name", shelf_name),
                    "description": shelf_detail.get("description", ""),
                    "books": merged_ids,
                },
            )
            logger.info(
                "Linked book to shelf using shelf update",
                extra={"shelf_id": shelf_id, "book_id": book_id},
            )
            return
        except Exception:
            logger.warning(
                "Primary shelf-link method failed, trying fallback methods",
                extra={"shelf_id": shelf_id, "book_id": book_id},
            )

        fallback_candidates = [
            ("POST", f"/api/shelves/{shelf_id}/books/{book_id}", None),
            ("POST", f"/api/shelves/{shelf_id}/books", {"book_id": book_id}),
        ]
        for method, path, payload in fallback_candidates:
            try:
                self._request(method, path, json=payload)
                logger.info(
                    "Linked book to shelf using fallback",
                    extra={"method": method, "path": path, "shelf_id": shelf_id, "book_id": book_id},
                )
                return
            except Exception:
                logger.warning(
                    "Shelf-link fallback failed",
                    extra={"method": method, "path": path, "shelf_id": shelf_id, "book_id": book_id},
                )

        raise RuntimeError(
            f"Failed to ensure book {book_id} in shelf {shelf_name!r}; check API permissions and shelf endpoints"
        )

    def _ensure_config_book(self, config_book_name: str) -> dict:
        logger.info("Ensuring config book and shelf setup", extra={"config_book_name": config_book_name})
        config_book = self._ensure_book(config_book_name)
        shelf = self._ensure_shelf(CONFIG_SHELF_NAME)
        self._ensure_book_in_shelf(
            shelf_id=int(shelf["id"]),
            shelf_name=CONFIG_SHELF_NAME,
            book_id=int(config_book["id"]),
        )
        logger.info(
            "Config book and shelf setup complete",
            extra={"config_book_name": config_book_name, "book_id": config_book.get("id")},
        )
        return config_book

    def _find_book_by_name(self, book_name: str) -> dict | None:
        book_name_cf = book_name.casefold()
        for book in self._list_all("/api/books"):
            if str(book.get("name", "")).casefold() == book_name_cf:
                return book
        return None

    def _find_chapter_by_name(self, *, book_id: int, chapter_name: str) -> dict | None:
        chapter_name_cf = chapter_name.casefold()
        for chapter in self._list_all("/api/chapters"):
            if int(chapter.get("book_id", -1)) != book_id:
                continue
            if str(chapter.get("name", "")).casefold() == chapter_name_cf:
                return chapter
        return None

    def _find_page_in_parent(
        self,
        *,
        page_name: str,
        book_id: int,
        chapter_id: int | None,
    ) -> dict | None:
        page_name_cf = page_name.casefold()
        for page in self._list_all("/api/pages"):
            if str(page.get("name", "")).casefold() != page_name_cf:
                continue

            page_book_id = page.get("book_id")
            page_chapter_id = page.get("chapter_id")

            if chapter_id is None and page_book_id == book_id and not page_chapter_id:
                return self._get_page(int(page["id"]))

            if chapter_id is not None and page_chapter_id == chapter_id:
                return self._get_page(int(page["id"]))

        return None

    def _create_book(self, name: str) -> dict:
        payload = self._request("POST", "/api/books", json={"name": name, "description": ""})
        if not isinstance(payload, dict):
            raise RuntimeError("Unexpected create book response")
        return payload

    def _create_chapter(self, *, book_id: int, name: str) -> dict:
        payload = self._request("POST", "/api/chapters", json={"name": name, "book_id": book_id})
        if not isinstance(payload, dict):
            raise RuntimeError("Unexpected create chapter response")
        return payload

    def _create_page(
        self,
        *,
        name: str,
        markdown: str,
        book_id: int,
        chapter_id: int | None = None,
    ) -> dict:
        payload: dict[str, object] = {
            "name": name,
            "markdown": markdown,
            "book_id": book_id,
        }
        if chapter_id is not None:
            payload["chapter_id"] = chapter_id

        created = self._request("POST", "/api/pages", json=payload)
        if not isinstance(created, dict):
            raise RuntimeError("Unexpected create page response")
        return created

    def _extract_page_id_from_link(self, link: str) -> int | None:
        match = re.search(r"/pages/(\d+)", link)
        if match:
            return int(match.group(1))

        parsed = urlparse(link)
        if parsed.path:
            match = re.search(r"/pages/(\d+)", parsed.path)
            if match:
                return int(match.group(1))
        return None

    def get_page_markdown(self, page_id: int) -> str:
        page = self._get_page(page_id)

        markdown = page.get("markdown")

        if markdown is None:
            raise RuntimeError(f"Page {page_id} does not include markdown content")

        return str(markdown)

    def get_or_create_config_page(self, config_book_name: str, config_page_name: str) -> dict:
        logger.info(
            "Ensuring config page exists",
            extra={"config_book_name": config_book_name, "config_page_name": config_page_name},
        )
        book = self._ensure_config_book(config_book_name)
        book_id = int(book["id"])

        existing = self._find_page_in_parent(
            page_name=config_page_name,
            book_id=book_id,
            chapter_id=None,
        )
        if existing is not None:
            logger.debug(
                "Config page already exists in config book",
                extra={"config_page_name": config_page_name, "book_id": book_id, "page_id": existing.get("id")},
            )
            return existing

        logger.info(
            "Config page not found in config book, creating example pages and config",
            extra={"config_page_name": config_page_name, "book_id": book_id},
        )
        random_suffix = random.randint(1000, 9999)
        source_a_name = f"orchestration_example_{random_suffix}_a"
        source_b_name = f"orchestration_example_{random_suffix}_b"
        source_c_name = f"orchestration_example_{random_suffix}_c"

        source_a_page = self._create_page(
            name=source_a_name,
            markdown="This is autogenerated source page A.",
            book_id=book_id,
        )
        source_b_page = self._create_page(
            name=source_b_name,
            markdown="This is autogenerated source page B.",
            book_id=book_id,
        )
        source_c_page = self._create_page(
            name=source_c_name,
            markdown="This is autogenerated shared source page C.",
            book_id=book_id,
        )

        target_ac_name = f"orchestration_example_{random_suffix}_ac"
        target_bc_name = f"orchestration_example_{random_suffix}_bc"
        config_markdown = (
            f"# {config_book_name}.{target_ac_name}\n\n"
            f"[Source A](/pages/{source_a_page['id']})\n\n"
            f"---\n\n"
            f"[Source C](/pages/{source_c_page['id']})\n\n"
            f"# {config_book_name}.{target_bc_name}\n\n"
            f"[Source B](/pages/{source_b_page['id']})\n\n"
            f"---\n\n"
            f"[Source C](/pages/{source_c_page['id']})\n"
        )

        config_page = self._create_page(name=config_page_name, markdown=config_markdown, book_id=book_id)
        logger.info(
            "Config page created with autogenerated examples",
            extra={"config_page_name": config_page_name, "config_page_id": config_page.get("id")},
        )
        return config_page

    def resolve_link_to_page(self, link: str) -> dict | None:
        page_id = self._extract_page_id_from_link(link)
        if page_id is not None:
            return self._get_page(page_id)

        parsed = urlparse(link)
        path = parsed.path or link
        slug_match = re.search(r"/page/([^/?#]+)", path)
        if not slug_match:
            return None

        slug = slug_match.group(1)
        for page in self._list_all("/api/pages"):
            if str(page.get("slug", "")) == slug:
                return self._get_page(int(page["id"]))

        return None

    def upsert_target_page(
        self,
        *,
        book_name: str,
        chapter_name: str | None,
        page_name: str,
        markdown: str,
    ) -> dict:
        book = self._ensure_book(book_name)

        book_id = int(book["id"])

        chapter_id: int | None = None
        if chapter_name:
            chapter = self._find_chapter_by_name(book_id=book_id, chapter_name=chapter_name)
            if chapter is None:
                chapter = self._create_chapter(book_id=book_id, name=chapter_name)
            chapter_id = int(chapter["id"])

        existing = self._find_page_in_parent(
            page_name=page_name,
            book_id=book_id,
            chapter_id=chapter_id,
        )
        if existing is not None:
            return existing

        return self._create_page(
            name=page_name,
            markdown=markdown,
            book_id=book_id,
            chapter_id=chapter_id,
        )

    def update_page_markdown(self, page_id: int, markdown: str) -> None:
        current_page = self._get_page(page_id)
        payload = {"markdown": markdown}
        if "name" in current_page:
            payload["name"] = current_page["name"]

        response = self._bookstack._session.request("PUT", f"/api/pages/{page_id}", json=payload)
        response.raise_for_status()
