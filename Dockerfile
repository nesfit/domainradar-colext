FROM ghcr.io/astral-sh/uv:0.9-alpine AS deps

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    UV_LINK_MODE=copy \
    UV_NO_DEV=1 \
    UV_PYTHON_INSTALL_DIR=/python \
    UV_PYTHON_PREFERENCE=only-managed

ENV LIBRD_VER=2.13.0

RUN uv python install python3.13 && \
    apk add --no-cache build-base unzip wget bash && \
    apk add --no-cache musl-dev zlib-dev openssl-dev zstd-dev pkgconfig libc-dev

RUN wget https://github.com/confluentinc/librdkafka/archive/refs/tags/v${LIBRD_VER}.zip && \
    unzip v${LIBRD_VER}.zip && \
    rm -rf v${LIBRD_VER}.zip && \
    cd librdkafka-${LIBRD_VER} && \
    STATIC_LIB_libzstd=/usr/lib/libzstd.a ./configure --prefix /usr --enable-static && \
    make -j$(nproc) && \
    make install && \
    make clean && \
    rm -rf librdkafka-${LIBRD_VER}

RUN --mount=type=cache,target=/root/.cache/uv \
    --mount=type=bind,source=uv.lock,target=uv.lock \
    --mount=type=bind,source=pyproject.toml,target=pyproject.toml \
    PKG_CONFIG_PATH=$PKG_CONFIG_PATH:/usr/lib/pkgconfig/ \
    uv sync --frozen --no-install-project --no-install-workspace
    #--mount=type=bind,source=kafka-multiprocessor/pyproject.toml,target=/app/kafka-multiprocessor/pyproject.toml \

COPY . /app

RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen

FROM alpine:3.23 AS runtime

# Setup a non-root user
ENV USER=collector
ENV GROUPNAME=$USER
ENV UID=12345
ENV GID=23456

RUN addgroup --gid "$GID" "$GROUPNAME" \
 && adduser \
    --disabled-password \
    --home "/app" \
    --ingroup "$GROUPNAME" \
    --no-create-home \
    --uid "$UID" \
    $USER

# Copy librdkafka
RUN apk add --no-cache zlib openssl zstd-libs
COPY --from=deps /usr/lib /usr/lib

# Copy the application from the builder
COPY --from=deps --chown=${USER}:${GROUPNAME} /python /python
COPY --from=deps --chown=${USER}:${GROUPNAME} /app /app

ENV THOR_COLLECTOR_MODULE=thor_collectors.zone
ENV PATH="/app/.venv/bin:$PATH"

# Use the non-root user to run our application
USER ${USER}

WORKDIR /app

ENTRYPOINT ["sh", "-c"]
CMD ["python -m ${THOR_COLLECTOR_MODULE}"]
