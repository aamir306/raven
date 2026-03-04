# RAVEN — Retrieval-Augmented Validated Engine for Natural-language SQL
# Multi-stage build for production deployment

FROM python:3.11-slim AS base

# System deps for psycopg2 (pgvector) and compilation
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libpq-dev \
    curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install Python dependencies first (layer caching)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Create non-root user
RUN useradd --create-home raven
USER raven

# FastAPI default port
EXPOSE 8000

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=10s --retries=3 \
    CMD curl -f http://localhost:8000/health || exit 1

CMD ["uvicorn", "src.raven.api:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "2"]
