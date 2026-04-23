# ============================================================================
# Vibe-Trading Railway Deploy — API only (no frontend build)
# ============================================================================
FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

COPY agent/requirements.txt agent/requirements.txt
RUN pip install --no-cache-dir -r agent/requirements.txt

COPY pyproject.toml LICENSE README.md ./
COPY agent/ agent/

RUN pip install --no-cache-dir -e .

RUN mkdir -p agent/runs agent/sessions agent/uploads

COPY docker-entrypoint.sh /docker-entrypoint.sh
RUN chmod +x /docker-entrypoint.sh

EXPOSE 8899

HEALTHCHECK --interval=15s --timeout=10s --start-period=120s --retries=5 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8899/health')" || exit 1

ENTRYPOINT ["/docker-entrypoint.sh"]
# Dùng shell form (không có []) để $PORT được expand đúng
CMD vibe-trading serve --host 0.0.0.0 --port ${PORT:-8899}
