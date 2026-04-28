# ── Stage 1: Builder ──────────────────────────────────────────────────────────
# MUST use 3.11 — pandas 2.2.2 has no pre-built wheel for Python 3.12+
FROM python:3.11-slim AS builder

WORKDIR /build

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    g++ \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir --prefix=/install -r requirements.txt


# ── Stage 2: Runtime ──────────────────────────────────────────────────────────
FROM python:3.11-slim AS runtime

RUN useradd --create-home --shell /bin/bash dataflow

WORKDIR /app

COPY --from=builder /install /usr/local
COPY --chown=dataflow:dataflow . .

RUN mkdir -p data && chown dataflow:dataflow data
RUN python generate_sample_data.py

USER dataflow

EXPOSE 8000 8501

ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1

CMD ["uvicorn", "api.main:app", "--host", "0.0.0.0", "--port", "8000"]
