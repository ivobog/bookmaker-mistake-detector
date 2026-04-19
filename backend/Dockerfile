FROM python:3.12-slim

WORKDIR /app

COPY pyproject.toml README.md ./
COPY src ./src

RUN pip install --no-cache-dir --default-timeout=120 setuptools wheel \
    && pip install --no-cache-dir --default-timeout=120 --no-build-isolation .

EXPOSE 8000

CMD ["uvicorn", "bookmaker_detector_api.main:app", "--host", "0.0.0.0", "--port", "8000"]
