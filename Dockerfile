FROM python:3.11-slim AS base

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

RUN mkdir -p /app/workspace

# ── Test target (used by CI) ──────────────────────────────
FROM base AS test
RUN pip install --no-cache-dir pytest pytest-asyncio
CMD ["python", "-m", "pytest", "tests/", "-v", "--tb=short", "--ignore=tests/test_mm.py", "-k", "not test_merge_skipped_below_threshold"]

# ── Production target ─────────────────────────────────────
FROM base AS production
EXPOSE 8000
CMD ["sh", "-c", "uvicorn web_server:app --host 0.0.0.0 --port ${PORT:-8000}"]
