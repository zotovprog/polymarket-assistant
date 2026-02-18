FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

COPY requirements.txt requirements-trading.txt ./
RUN pip install --no-cache-dir -r requirements.txt -r requirements-trading.txt

COPY . .

RUN mkdir -p /app/workspace

EXPOSE 8000

CMD ["uvicorn", "web_server:app", "--host", "0.0.0.0", "--port", "8000"]
