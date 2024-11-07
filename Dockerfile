FROM python:3.9-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    bash \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Create necessary directories
RUN mkdir -p /app/data

COPY . .

ENV PYTHONUNBUFFERED=1

# Make sure the data directory exists and is writable
RUN chmod -R 777 /app/data

CMD ["python", "-u", "app/app.py"] 