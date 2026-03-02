from __future__ import annotations

from fastapi.testclient import TestClient

from bookstack_page_orchestrator.main import create_app


class StubOrchestrator:
    def __init__(self, response: dict) -> None:
        self.response = response
        self.payloads: list[dict] = []

    def process_webhook(self, payload: dict):
        self.payloads.append(payload)

        class _Result:
            def __init__(self, ignored, reason, updated_targets):
                self.ignored = ignored
                self.reason = reason
                self.updated_targets = updated_targets

        return _Result(
            ignored=self.response["ignored"],
            reason=self.response["reason"],
            updated_targets=self.response["updated_targets"],
        )


def test_webhook_returns_orchestration_result(monkeypatch) -> None:
    monkeypatch.setenv("BOOKSTACK_URL", "https://bookstack.example.com")
    monkeypatch.setenv("BOOKSTACK_TOKEN_ID", "id")
    monkeypatch.setenv("BOOKSTACK_TOKEN_SECRET", "secret")
    monkeypatch.setenv("CONFIG_BOOK_NAME", "Bookstack Orchestrator Config Book")
    monkeypatch.setenv("CONFIG_PAGE_NAME", "Bookstack Orchestrator Config")
    monkeypatch.setenv("PAGE_RECIPES", "{\"200\": [\"100\"]}")

    app = create_app()
    stub = StubOrchestrator(
        {"ignored": False, "reason": None, "updated_targets": [200]},
    )
    app.state.orchestrator = stub
    client = TestClient(app)

    response = client.post("/webhook", json={"event": "page_update", "data": {"id": 100}})

    assert response.status_code == 200
    assert response.json() == {"accepted": True}
    assert stub.payloads == [{"event": "page_update", "data": {"id": 100}}]
