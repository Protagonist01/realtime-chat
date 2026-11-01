FROM python:3.15-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt \
    && groupadd --gid 10001 chat \
    && useradd --uid 10001 --gid chat --no-create-home chat \
    && mkdir /data && chown chat:chat /data

COPY --chown=chat:chat app/ ./app/
COPY --chown=chat:chat static/ ./static/
COPY --chown=chat:chat migrations/ ./migrations/
COPY --chown=chat:chat alembic.ini .

USER chat
ENV DATABASE_URL=sqlite+aiosqlite:////data/chat.db
EXPOSE 8000

CMD ["sh", "-c", "exec uvicorn app.main:app --host 0.0.0.0 --port ${PORT:-8000} --ws-max-size 20000"]
