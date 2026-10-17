# Backend container (FastAPI + ML pipeline). Optional for local use (`docker compose up api`);
# the same image runs the staging backend (docs/DEPLOYMENT.md).
FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_DEFAULT_TIMEOUT=180 \
    PIP_RETRIES=10 \
    API_HOST=0.0.0.0 \
    API_PORT=8000 \
    LOG_FORMAT=json

WORKDIR /app

# Dependencies first (cached layer). pyproject + constraints define the exact versions.
COPY pyproject.toml README.md LICENSE ./
COPY requirements/constraints.txt requirements/constraints.txt
COPY pipeline/icp_ml/__init__.py pipeline/icp_ml/__init__.py
COPY backend/icp_api/__init__.py backend/icp_api/__init__.py
# tesseract-ocr: OCR fallback for scanned ESDM decrees (text-layer PDFs and announcements need no OCR).
RUN apt-get update && apt-get install -y --no-install-recommends tesseract-ocr && rm -rf /var/lib/apt/lists/*
RUN pip install -e ".[api,cloud,postgres,ingest]" -c requirements/constraints.txt

# Application source, schema migrations and the seed datasets (DVC-managed; must exist locally).
COPY pipeline pipeline
COPY backend backend
COPY infra/cloudflare/d1/migrations infra/cloudflare/d1/migrations
COPY data/raw/dataset.csv data/raw/wti.csv data/raw/
COPY data/seed data/seed

RUN useradd --create-home --uid 10001 app && mkdir -p /app/var && chown -R app:app /app/var
USER app

EXPOSE 8000
HEALTHCHECK --interval=15s --timeout=5s --start-period=30s --retries=5 \
  CMD python -c "import os,urllib.request,sys; p=os.environ.get('PORT') or os.environ.get('API_PORT','8000'); sys.exit(0 if urllib.request.urlopen(f'http://127.0.0.1:{p}/health/live', timeout=4).status == 200 else 1)"

CMD ["python", "-m", "icp_api.cli", "serve"]
