# ── Stage 1: Builder ─────────────────────────────────────────────────────────
FROM python:3.11-slim AS builder

WORKDIR /build

# Install build deps — needed for some pandas/numpy wheels
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    g++ \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .

# Install into a prefix directory we can copy to final image
RUN pip install --no-cache-dir --prefix=/install -r requirements.txt


# ── Stage 2: Runtime ─────────────────────────────────────────────────────────
FROM python:3.11-slim AS runtime

# Non-root user for security
RUN useradd --create-home --shell /bin/bash dataflow

WORKDIR /app

# Copy installed packages from builder
COPY --from=builder /install /usr/local

# Copy application code
COPY --chown=dataflow:dataflow . .

# Create data directory for SQLite files
RUN mkdir -p data && chown dataflow:dataflow data

# Generate sample data on build
RUN python generate_sample_data.py

USER dataflow

# Expose ports for API and dashboard
EXPOSE 8000 8501

ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1

# Default: run the API server
# Override CMD in docker-compose for scheduler/dashboard
CMD ["uvicorn", "api.main:app", "--host", "0.0.0.0", "--port", "8000"]
