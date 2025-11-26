# backend/database/Dockerfile
FROM python:3.11

WORKDIR /app

# Copy code
COPY . .

# Cài system package cần thiết cho psycopg2, pandas
RUN apt-get update && \
    apt-get install -y gcc libpq-dev && \
    rm -rf /var/lib/apt/lists/*

# Cài Python packages trực tiếp
RUN pip install --no-cache-dir fastapi uvicorn pandas sqlalchemy psycopg2-binary

# Run FastAPI
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "7001"]
