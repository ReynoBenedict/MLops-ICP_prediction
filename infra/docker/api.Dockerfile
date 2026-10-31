# Backend container (FastAPI + ML pipeline). Optional for local use (`docker compose up api`);
# the same image runs the staging backend (docs/DEPLOYMENT.md).
# Base image pinned by digest for reproducible builds (multi-arch index of python:3.11-slim-bookworm,
# Python 3.11.17, published 2026-10-06; read with `docker buildx imagetools inspect` on 2026-10-20).
# To take OS/Python security updates: re-read the digest, rebuild, run the test suite, then bump here.
ARG PYTHON_IMAGE=python:3.11-slim-bookworm@sha256:0a310eeecf4e1f5a0743f9a6520c90c88d089c903ca5fd283f501e3a805f5f89
FROM ${PYTHON_IMAGE}

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

# Application source, schema migrations and the seed datasets. All are Git-tracked: the ICP history
# (data/raw/dataset.csv) and the EIA-based seeds (data/seed/*.csv; data/seed/wti.csv is byte-identical to the
# active WTI dataset version and to the series production model v5 was trained on). The DVC-only legacy
# data/raw/wti.csv is deliberately NOT copied (audit v7 DATA-06); SEED_WTI_PATH pins the seed explicitly.
COPY pipeline pipeline
COPY backend backend
COPY infra/cloudflare/d1/migrations infra/cloudflare/d1/migrations
COPY data/raw/dataset.csv data/raw/
COPY data/seed/ data/seed/
ENV SEED_WTI_PATH=/app/data/seed/wti.csv

RUN useradd --create-home --uid 10001 app && mkdir -p /app/var && chown -R app:app /app/var
USER app

EXPOSE 8000
HEALTHCHECK --interval=15s --timeout=5s --start-period=30s --retries=5 \
  CMD python -c "import os,urllib.request,sys; p=os.environ.get('PORT') or os.environ.get('API_PORT','8000'); sys.exit(0 if urllib.request.urlopen(f'http://127.0.0.1:{p}/health/live', timeout=4).status == 200 else 1)"

CMD ["python", "-m", "icp_api.cli", "serve"]
