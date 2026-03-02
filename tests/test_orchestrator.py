from __future__ import annotations

from bookstack_page_orchestrator.orchestrator import (
    PageOrchestrator,
    parse_config_markdown,
    render_template,
)


class FakePageClient:
    def __init__(self, config_page: dict, pages: dict[int, str]) -> None:
        self.config_page = config_page
        self.pages = pages
        self.link_map: dict[str, dict] = {}
        self.target_map: dict[tuple[str, str | None, str], dict] = {}
        self.updated: list[tuple[int, str]] = []
        self.created_config_calls = 0

    def get_page_markdown(self, page_id: int) -> str:
        return self.pages[page_id]

    def get_or_create_config_page(self, config_book_name: str, config_page_name: str) -> dict:
        self.created_config_calls += 1
        return self.config_page

    def resolve_link_to_page(self, link: str) -> dict | None:
        return self.link_map.get(link)

    def upsert_target_page(
        self,
        *,
        book_name: str,
        chapter_name: str | None,
        page_name: str,
        markdown: str,
    ) -> dict:
        key = (book_name, chapter_name, page_name)
        if key not in self.target_map:
            self.target_map[key] = {"id": len(self.target_map) + 900, "name": page_name}
        return self.target_map[key]

    def update_page_markdown(self, page_id: int, markdown: str) -> None:
        self.updated.append((page_id, markdown))


def test_parse_config_markdown_supports_book_and_chapter_targets() -> None:
    markdown = """
# Book A.Page A
A body

# Book B.Chapter B.Page B
B body
""".strip()

    rules = parse_config_markdown(markdown)

    assert len(rules) == 2
    assert (rules[0].book_name, rules[0].chapter_name, rules[0].page_name) == (
        "Book A",
        None,
        "Page A",
    )
    assert (rules[1].book_name, rules[1].chapter_name, rules[1].page_name) == (
        "Book B",
        "Chapter B",
        "Page B",
    )


def test_render_template_replaces_internal_links_with_page_markdown() -> None:
    client = FakePageClient(config_page={"id": 1, "markdown": ""}, pages={10: "Source One", 11: "Source Two"})
    client.link_map = {
        "/pages/10": {"id": 10},
        "/pages/11": {"id": 11},
    }

    rendered = render_template("Before [A](/pages/10) middle [B](/pages/11) after", client)

    assert rendered.markdown == "Before Source One middle Source Two after"
    assert rendered.source_page_ids == {10, 11}


def test_ignores_unsupported_event() -> None:
    orchestrator = PageOrchestrator(
        FakePageClient(config_page={"id": 1, "markdown": "# A.B\nX"}, pages={}),
        config_book_name="Config Book",
        config_page_name="Config",
    )

    result = orchestrator.process_webhook({"event": "book_update", "data": {"id": 100}})

    assert result.ignored is True
    assert result.reason == "unsupported_event"
    assert result.updated_targets == []


def test_initialize_creates_updates_from_config_rules() -> None:
    config = {
        "id": 5,
        "markdown": "# Book.Target\n[A](/pages/10)\n\n---\n\n[B](/pages/11)",
    }
    client = FakePageClient(config_page=config, pages={10: "Alpha", 11: "Beta"})
    client.link_map = {"/pages/10": {"id": 10}, "/pages/11": {"id": 11}}

    orchestrator = PageOrchestrator(client, config_book_name="Config Book", config_page_name="Config")

    result = orchestrator.initialize()

    assert result.ignored is False
    assert result.updated_targets == [900]
    assert client.updated == [(900, "Alpha\n\n---\n\nBeta")]


def test_webhook_skips_when_changed_page_is_target_page() -> None:
    config = {
        "id": 5,
        "markdown": "# Book.Target\n[A](/pages/10)",
    }
    client = FakePageClient(config_page=config, pages={10: "Alpha"})
    client.link_map = {"/pages/10": {"id": 10}}
    client.target_map = {("Book", None, "Target"): {"id": 900, "name": "Target"}}
    orchestrator = PageOrchestrator(client, config_book_name="Config Book", config_page_name="Config")

    result = orchestrator.process_webhook({"event": "page_update", "page_id": 900})

    assert result.updated_targets == []
    assert client.updated == []


def test_webhook_updates_when_changed_page_is_source_page() -> None:
    config = {
        "id": 5,
        "markdown": "# Book.Target\n[A](/pages/10)",
    }
    client = FakePageClient(config_page=config, pages={10: "Alpha"})
    client.link_map = {"/pages/10": {"id": 10}}
    orchestrator = PageOrchestrator(client, config_book_name="Config Book", config_page_name="Config")

    result = orchestrator.process_webhook({"event": "page_update", "related_item": {"id": 10}})

    assert result.updated_targets == [900]
    assert client.updated == [(900, "Alpha")]
