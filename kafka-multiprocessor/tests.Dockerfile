FROM python:3.13-slim

WORKDIR /work

COPY pyproject.toml /work/pyproject.toml
COPY README.md /work/README.md
COPY src /work/src

RUN pip install --no-cache-dir -e ".[dev]"

CMD ["pytest"]
