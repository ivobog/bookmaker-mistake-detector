FROM python:3.12-slim

WORKDIR /app

COPY worker/pyproject.toml ./pyproject.toml
COPY worker/README.md ./README.md
COPY worker/src ./src
COPY backend/src ./backend/src

ENV PYTHONPATH=/app/backend/src

RUN pip install --no-cache-dir --default-timeout=120 setuptools wheel \
    && pip install --no-cache-dir --default-timeout=120 --no-build-isolation .

CMD ["python", "-m", "bookmaker_detector_worker.main"]
