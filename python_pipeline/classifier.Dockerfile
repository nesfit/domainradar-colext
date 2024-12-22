ARG TARGET_UNIT="classifier_v2"
ARG TARGET_MODULE="classifier_unit"

FROM python:3.11-slim-bookworm AS python-base
ENV PYTHONUNBUFFERED=1 \
    # prevents python creating .pyc files
    PYTHONDONTWRITEBYTECODE=1 \
    # pip
    PIP_DISABLE_PIP_VERSION_CHECK=on \
    PIP_DEFAULT_TIMEOUT=100 \
    # poetry
    POETRY_VERSION=1.8.2 \
    POETRY_HOME="/opt/poetry" \
    POETRY_VIRTUALENVS_IN_PROJECT=1 \
    POETRY_VIRTUALENVS_CREATE=1 \
    POETRY_NO_INTERACTION=1 \
    POETRY_CACHE_DIR="/tmp/poetry_cache" \
    PYSETUP_PATH="/opt/pysetup" \
    VENV_PATH="/opt/pysetup/.venv"
ENV PATH="$POETRY_HOME/bin:$VENV_PATH/bin:$PATH"

FROM python-base AS poetry-base

# Install packages
RUN apt-get update && \
    apt-get install --no-install-recommends -y \
    # deps for installing poetry
    curl \
    # deps for pulling deps from git
    git openssh-client \
    # deps for building python deps
    build-essential && \
    rm -rf /var/lib/apt/lists/*

# Install poetry
RUN --mount=type=cache,target=$POETRY_CACHE_DIR \
    curl -sSL https://install.python-poetry.org | python3 -

FROM poetry-base AS builder
ARG TARGET_UNIT

# Copy project requirement files here to ensure they will be cached
WORKDIR $PYSETUP_PATH
COPY ${TARGET_UNIT}/pyproject.toml ./
COPY ./domainradar-clf/pyproject.toml ../domainradar-clf/pyproject.toml

# Install runtime deps (uses $POETRY_VIRTUALENVS_IN_PROJECT internally)
RUN --mount=type=cache,target=$POETRY_CACHE_DIR  \
    poetry install --without=dev --no-directory --no-root

FROM python-base AS production
# Consume the build argument
ARG TARGET_UNIT
ARG TARGET_MODULE
ENV TARGET_MODULE=${TARGET_MODULE}
ENV APP_DATADIR=/app/faust_data

COPY --from=builder $PYSETUP_PATH $PYSETUP_PATH
COPY ./${TARGET_UNIT} /app
# This is fugly, must be fixed
COPY ./domainradar-clf/classifiers /opt/pysetup/.venv/lib/python3.11/site-packages/classifiers
COPY ./common /app/common
COPY ./docker_entrypoint.sh /app/docker_entrypoint.sh
RUN mkdir /app/faust_data

WORKDIR /app
ENTRYPOINT ./docker_entrypoint.sh $TARGET_MODULE
