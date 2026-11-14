FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app
COPY simulator ./simulator

# Web 服务（FastAPI）默认入口；模拟器服务在 compose 里覆盖 command
EXPOSE 8000 9000 9001

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
