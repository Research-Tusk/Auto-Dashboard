# ===========================================================================
# AutoQuant ETL — Dockerfile
# Python 3.11 + Playwright Chromium
# ===========================================================================

FROM python:3.11-slim-bullseye

# System deps for Playwright + Chromium
RUN apt-get update && apt-get install -y \
    wget curl gnupg \
    libnss3 libatk1.0-0 libatk-bridge2.0-0 libcups2 \
    libxkbcommon0 libxcomposite1 libxdamage1 libxfixes3 \
    libxrandr2 libgbm1 libpango-1.0-0 libcairo2 libasound2 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Install Playwright + Chromium browser
RUN playwright install chromium
RUN playwright install-deps chromium

# Copy ETL source
COPY autoquant_etl/ ./autoquant_etl/
COPY seeds/ ./seeds/

# Default command (override in docker-compose / Railway / Render)
CMD ["python", "-m", "autoquant_etl", "health"]
