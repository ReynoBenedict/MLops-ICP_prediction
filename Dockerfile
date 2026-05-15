# =============================================================
# Dockerfile — ICP Price Prediction Streamlit Application
# =============================================================

FROM python:3.10-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# System dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Python dependencies (cached layer)
COPY requirements.txt .
RUN pip install --no-cache-dir --default-timeout=100 -r requirements.txt

# Copy application source
COPY config/ ./config/
COPY src/ ./src/
COPY services/ ./services/
COPY components/ ./components/
COPY utils/ ./utils/
COPY pages/ ./pages/
COPY data/ ./data/
COPY .streamlit/ ./.streamlit/
COPY app.py .
COPY audit_and_promote.py .

EXPOSE 8501

CMD ["streamlit", "run", "app.py", "--server.port=8501", "--server.address=0.0.0.0"]
