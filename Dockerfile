FROM python:3.11-slim-bookworm

# Принудительно переписываем конфиг репозиториев на Яндекс (новый формат Debian 12)
RUN echo "Types: deb\n\
URIs: http://mirror.yandex.ru/debian/\n\
Suites: bookworm bookworm-updates\n\
Components: main\n\
Signed-By: /usr/share/keyrings/debian-archive-keyring.gpg\n\
\n\
Types: deb\n\
URIs: http://mirror.yandex.ru/debian-security\n\
Suites: bookworm-security\n\
Components: main\n\
Signed-By: /usr/share/keyrings/debian-archive-keyring.gpg" > /etc/apt/sources.list.d/debian.sources && \
    echo 'Acquire::ForceIPv4 "true";' > /etc/apt/apt.conf.d/99force-ipv4

# Теперь установка пойдет через зеркала РФ и по IPv4
RUN apt-get update && apt-get install -y \
    build-essential \
    libpq-dev \
    supervisor \
    curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY requirements.txt .
# Копируем зависимости
# Используем стабильное и полное зеркало
RUN pip install --no-cache-dir \
    --index-url https://pypi.tuna.tsinghua.edu.cn/simple \
    --default-timeout=1000 \
    -r requirements.txt

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