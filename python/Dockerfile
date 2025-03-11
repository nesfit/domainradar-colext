ARG TARGET_UNIT="collector"
ARG TARGET_MODULE="collectors.rtt"

FROM docker.io/library/python:3.11-bookworm AS python-base
ENV PYTHONUNBUFFERED=1 \
    # pip
    PIP_DISABLE_PIP_VERSION_CHECK=on \
    PIP_DEFAULT_TIMEOUT=100 \
    # poetry
    POETRY_VERSION=1.8.2 \
    POETRY_NO_INTERACTION=1 \
    POETRY_HOME="/opt/poetry" \
    POETRY_CACHE_DIR="/tmp/poetry_cache" \
    POETRY_VIRTUALENVS_CREATE=1 \
    POETRY_VIRTUALENVS_IN_PROJECT=1
ENV PATH="$POETRY_HOME/bin:$PATH"

FROM python-base AS poetry-base

# Install packages
RUN apt-get update && \
    apt-get install --no-install-recommends -y \
    # deps for installing poetry
    curl \
    # deps for pulling deps from git
    git openssh-client \
    # deps for building python deps
    build-essential \
    # certificates
    ca-certificates && \
    update-ca-certificates && \
    # clean package cache
    rm -rf /var/lib/apt/lists/*

# Install poetry
RUN --mount=type=cache,target=$POETRY_CACHE_DIR \
    curl -sSL https://install.python-poetry.org | python3 -

FROM poetry-base AS builder
ARG TARGET_UNIT

WORKDIR /app
COPY ${TARGET_UNIT}/pyproject.toml ${TARGET_UNIT}/poetry.lock* ./${TARGET_UNIT}/
COPY domrad_kafka_client/pyproject.toml domrad_kafka_client/poetry.lock  domrad_kafka_client/*.md ./domrad_kafka_client/
RUN mkdir common && touch common/__init__.py && \ 
    mkdir domrad_kafka_client/domrad_kafka_client && touch domrad_kafka_client/domrad_kafka_client/__init__.py
        
WORKDIR /app/${TARGET_UNIT}
RUN --mount=type=cache,target=$POETRY_CACHE_DIR \
    poetry install --no-dev

FROM python-base AS production
# Consume the build argument
ARG TARGET_UNIT
ARG TARGET_MODULE
ENV TARGET_MODULE=${TARGET_MODULE}

COPY --from=builder /app/${TARGET_UNIT}/.venv /app/${TARGET_UNIT}/.venv

WORKDIR /app
COPY ./docker_entrypoint.sh ./${TARGET_UNIT}/docker_entrypoint.sh
COPY ./domrad_kafka_client ./domrad_kafka_client
COPY ./common ./${TARGET_UNIT}/common
COPY ./${TARGET_UNIT} ./${TARGET_UNIT}

ENV PATH="/app/${TARGET_UNIT}/.venv/bin:$PATH"

WORKDIR /app/${TARGET_UNIT}
ENTRYPOINT [ "/bin/bash", "-c", "./docker_entrypoint.sh $TARGET_MODULE"]
