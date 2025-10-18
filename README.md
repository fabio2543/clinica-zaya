
# Streamlit + Python + Docker Starter (Zaya)

Ambiente mínimo para iniciar rápido com Streamlit, Python e Docker.

## Requisitos
- Docker + Docker Compose
- (Opcional) Make

## Como rodar
1. Copie `.env.example` para `.env` e ajuste as variáveis (porta, etc.).
   ```bash
   cp .env.example .env
   ```
2. Construa e suba os containers:
   ```bash
   docker compose build
   docker compose up
   ```
   Abra: http://localhost:8501

3. Edite seu app em `src/app.py` e crie páginas em `src/pages/`.

## Dicas
- **Hot reload**: o Streamlit recarrega ao salvar.
- **Bibliotecas**: adicione no `requirements.txt` e rode `docker compose build` novamente.
- **Planilhas**: faça upload no sidebar para inspecionar.
- **Lint/Format**:
  - `make lint` (ruff) e `make fmt` (black) — requer Docker.
