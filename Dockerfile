FROM python:3.11-slim-bookworm

# Установка системных зависимостей + Supervisor
# curl нужен для healthcheck, libpq-dev для postgres
RUN apt-get update && apt-get install -y \
    build-essential \
    libpq-dev \
    supervisor \
    curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Копируем зависимости
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Копируем код
COPY . .

# Копируем конфиг супервизора
COPY supervisord.conf /etc/supervisor/conf.d/supervisord.conf

# Создаем папку для логов внутри контейнера
RUN mkdir -p /app/logs

# Переменные окружения для Python (чтобы логи сразу летели в stdout/file без буферизации)
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Запускаем Supervisor, который запустит все остальное
CMD ["/usr/bin/supervisord", "-c", "/etc/supervisor/conf.d/supervisord.conf"]