#!/usr/bin/env sh
set -e

APP_ENTRY="${APP_ENTRY:-src/main.py}"
exec streamlit run "$APP_ENTRY" --server.port=8501 --server.address=0.0.0.0


if [ "${ENABLE_DEBUGPY:-0}" = "1" ]; then
  python -m pip install debugpy >/dev/null 2>&1 || true
  exec python -m debugpy --listen 0.0.0.0:5678 ${WAIT_FOR_CLIENT:+--wait-for-client} \
    -m streamlit run "$APP_ENTRY" --server.port=8501 --server.address=0.0.0.0
else
  exec streamlit run "$APP_ENTRY" --server.port=8501 --server.address=0.0.0.0
fi
