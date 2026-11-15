"""Uvicorn entrypoint."""

from __future__ import annotations

import uvicorn

from conduit.app import create_app
from conduit.settings import Settings

app = create_app()


def main() -> None:
    settings = Settings()
    uvicorn.run(app, host=settings.host, port=settings.port)


if __name__ == "__main__":
    main()
