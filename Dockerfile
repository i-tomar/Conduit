FROM python:3.12-slim-bookworm

COPY --from=ghcr.io/astral-sh/uv:0.13.8 /uv /uvx /bin/

WORKDIR /app
ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_NO_DEV=1 \
    PYTHONUNBUFFERED=1 \
    PATH="/app/.venv/bin:$PATH"

COPY pyproject.toml uv.lock README.md ./
COPY src ./src
COPY config ./config

RUN uv sync --frozen --no-dev \
    && useradd --create-home --uid 10001 conduit \
    && chown -R conduit:conduit /app

USER conduit
EXPOSE 8080
CMD ["uvicorn", "conduit.main:app", "--host", "0.0.0.0", "--port", "8080"]
