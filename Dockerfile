ARG PYTHON_VERSION=3.12

FROM ghcr.io/astral-sh/uv:0.13.7 AS uv

FROM python:$PYTHON_VERSION
COPY --from=uv /uv /uvx /bin/

ENV PYTHONUNBUFFERED=1
ENV UV_COMPILE_BYTECODE=1
ENV UV_LINK_MODE=copy

COPY ./pyproject.toml ./README.md ./LICENSE /src/
COPY ./fastpubsub/__init__.py /src/fastpubsub/__init__.py

WORKDIR /src

RUN uv sync --group dev

ENV PATH="/src/.venv/bin:$PATH"
