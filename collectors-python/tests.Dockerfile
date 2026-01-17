FROM python:3.13-slim

WORKDIR /work

COPY pyproject.toml /work/pyproject.toml
COPY kafka-multiprocessor/pyproject.toml /work/kafka-multiprocessor/pyproject.toml

COPY README.md /work/README.md
COPY kafka-multiprocessor/README.md /work/kafka-multiprocessor/README.md

RUN mkdir -p /work/kafka-multiprocessor/src/kafka_multiprocessor /work/src/thor_collectors
RUN touch /work/kafka-multiprocessor/src/kafka_multiprocessor/__init__.py && \
    touch /work/src/thor_collectors/__init__.py

RUN cd /work/kafka-multiprocessor && pip install --no-cache-dir -e ".[dev]"
RUN cd /work && pip install --no-cache-dir -e ".[dev]"

CMD ["pytest"]
