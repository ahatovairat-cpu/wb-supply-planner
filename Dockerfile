FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

ENV PORT=10000
EXPOSE 10000

# gunicorn: 2 воркера, таймаут 120с (WB API бывает медленный)
CMD gunicorn --bind 0.0.0.0:${PORT} --workers 2 --timeout 120 --access-logfile - --error-logfile - app:app
