FROM python:3.11-slim-bookworm

# 1. Заменяем стандартные зеркала на Яндекс (проверяем оба возможных пути конфига)
# 2. Форсируем IPv4, чтобы не ждать таймаутов IPv6 (частая причина тормозов)
RUN sed -i 's/deb.debian.org/mirror.yandex.ru/g' /etc/apt/sources.list.d/debian.sources || \
    sed -i 's/deb.debian.org/mirror.yandex.ru/g' /etc/apt/sources.list && \
    echo 'Acquire::ForceIPv4 "true";' > /etc/apt/apt.conf.d/99force-ipv4

# Дальше твой код без изменений, но работать будет в разы быстрее
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