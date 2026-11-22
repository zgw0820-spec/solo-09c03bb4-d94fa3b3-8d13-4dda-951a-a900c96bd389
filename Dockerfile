FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /srv

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app
COPY data ./data

# Container state should survive restarts of the web app; runtime settings are
# written here and shared with the lazily spawned backend child.
RUN mkdir -p /srv/data/state
ENV BACKEND_DATA_DIR=/srv/data/state \
    BACKEND_LOG_FILE=/srv/data/sample_logs.jsonl \
    BACKEND_DRILL_LOG_FILE=/srv/data/drill_gateway_logs.jsonl

EXPOSE 8000
HEALTHCHECK --interval=10s --timeout=3s --start-period=5s --retries=3 \
    CMD python -c "import urllib.request,sys; sys.exit(0 if urllib.request.urlopen('http://127.0.0.1:8000/api/health', timeout=2).status == 200 else 1)"

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
