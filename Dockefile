ARG PYTHON_VERSION=3.12

FROM python:$PYTHON_VERSION
COPY --from=ghcr.io/astral-sh/uv:0.9.18 /uv /uvx /bin/

ENV PYTHONUNBUFFERED=1

COPY ./pyproject.toml ./README.md ./LICENSE /src/
COPY ./fastpubsub/__init__.py /src/fastpubsub/__init__.py

WORKDIR /src

RUN uv sync --group dev --all-extras

ENV PATH="/src/.venv/bin:$PATH"
