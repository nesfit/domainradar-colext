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

FROM python-base AS builder

RUN apt-get update && \
    apt-get install --no-install-recommends -y \
    # deps for installing poetry
    curl \
    # deps for building python deps
    build-essential && \
    rm -rf /var/lib/apt/lists/*

# Install poetry
RUN --mount=type=cache,target=$POETRY_CACHE_DIR \
    curl -sSL https://install.python-poetry.org | python3 -

# Copy project requirement files here to ensure they will be cached
WORKDIR $PYSETUP_PATH
COPY poetry.lock pyproject.toml ./

# Install runtime deps (uses $POETRY_VIRTUALENVS_IN_PROJECT internally)
RUN --mount=type=cache,target=$POETRY_CACHE_DIR \
    poetry install --without=dev

FROM python-base AS development
ENV APP_DEBUG=True
WORKDIR $PYSETUP_PATH

# Copy in our built poetry + venv
COPY --from=builder $POETRY_HOME $POETRY_HOME
COPY --from=builder $PYSETUP_PATH $PYSETUP_PATH

# Install the dev dependencies
RUN --mount=type=cache,target=$POETRY_CACHE_DIR \
    poetry install --with=dev

# The code should be mounted to /app
WORKDIR /app
ENTRYPOINT ["/bin/bash"]
CMD ["echo", "Please provide a command to run"]

FROM python-base AS production
COPY --from=builder $PYSETUP_PATH $PYSETUP_PATH
COPY . /app/
WORKDIR /app
ENTRYPOINT ["/bin/bash", "docker_entrypoint.sh"]