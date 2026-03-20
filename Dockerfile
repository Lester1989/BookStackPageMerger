FROM ghcr.io/astral-sh/uv:python3.13-bookworm-slim AS builder

WORKDIR /app

COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev --no-install-project

FROM python:3.13-slim-bookworm AS runtime

WORKDIR /app

ENV VIRTUAL_ENV=/opt/venv
ENV PATH="/opt/venv/bin:$PATH"
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONPATH=/app/src

COPY --from=builder /app/.venv /opt/venv
COPY src ./src

EXPOSE 8000

CMD ["python", "-m", "uvicorn", "bookstack_page_orchestrator.main:app", "--host", "0.0.0.0", "--port", "8000"]
