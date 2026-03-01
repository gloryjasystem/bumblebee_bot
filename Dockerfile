FROM python:3.12-slim

WORKDIR /app

# Системные зависимости
RUN apt-get update && apt-get install -y \
    libpq-dev gcc \
    && rm -rf /var/lib/apt/lists/*

# Python зависимости
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Код приложения
COPY . .

# Railway слушает на порту $PORT (обычно 8080)
ENV PORT=8000
EXPOSE $PORT

CMD ["python", "bot.py"]
