# syntax=docker/dockerfile:1
ARG BASE_IMAGE=python:3.11.9-slim
FROM ${BASE_IMAGE}

# Evita cache e melhora logs
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

ENV PYTHONPATH="${PYTHONPATH}:/app"

# Instalar dependências básicas
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copiar dependências primeiro (para cache eficiente)
COPY requirements.txt .

RUN pip install --upgrade pip && pip install -r requirements.txt

# Copiar todo o restante do projeto
COPY src ./src
COPY .streamlit ./.streamlit
COPY entrypoint.sh ./entrypoint.sh
RUN sed -i 's/\r$//' /app/entrypoint.sh && chmod +x /app/entrypoint.sh

# Expor portas do Streamlit e debug
EXPOSE 8501
EXPOSE 5678

# Iniciar via script
ENTRYPOINT ["./entrypoint.sh"]
