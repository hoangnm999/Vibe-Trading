#!/bin/sh
# docker-entrypoint.sh — tạo agent/.env từ Railway env vars rồi chạy app

set -e

ENV_FILE="/app/agent/.env"

echo "[entrypoint] Generating $ENV_FILE from environment..."

cat > "$ENV_FILE" << ENVEOF
# Auto-generated from Railway environment variables
LANGCHAIN_PROVIDER=${LANGCHAIN_PROVIDER:-deepseek}
LANGCHAIN_MODEL_NAME=${LANGCHAIN_MODEL_NAME:-deepseek-chat}

DEEPSEEK_API_KEY=${DEEPSEEK_API_KEY:-}
DEEPSEEK_BASE_URL=${DEEPSEEK_BASE_URL:-https://api.deepseek.com/v1}

OPENAI_API_KEY=${OPENAI_API_KEY:-}
OPENAI_BASE_URL=${OPENAI_BASE_URL:-https://api.openai.com/v1}

GEMINI_API_KEY=${GEMINI_API_KEY:-}
OPENROUTER_API_KEY=${OPENROUTER_API_KEY:-}
OPENROUTER_BASE_URL=${OPENROUTER_BASE_URL:-https://openrouter.ai/api/v1}

API_AUTH_KEY=${API_AUTH_KEY:-}
CORS_ORIGINS=${CORS_ORIGINS:-*}
ENABLE_SESSION_RUNTIME=${ENABLE_SESSION_RUNTIME:-true}
ENVEOF

echo "[entrypoint] agent/.env written."
echo "[entrypoint] Provider : ${LANGCHAIN_PROVIDER:-deepseek}"
echo "[entrypoint] Model    : ${LANGCHAIN_MODEL_NAME:-deepseek-chat}"
echo "[entrypoint] Port     : ${PORT:-8899}"
echo "[entrypoint] Auth Key : $([ -n "$API_AUTH_KEY" ] && echo 'SET' || echo 'NOT SET (dev mode)')"
echo "[entrypoint] Starting vibe-trading serve..."

exec "$@"
