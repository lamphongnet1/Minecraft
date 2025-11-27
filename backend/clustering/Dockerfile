# backend/clustering/Dockerfile
FROM python:3.11

WORKDIR /app

# Copy requirements trước để cache layer pip install
COPY requirements.txt .

# Cài system packages cần thiết
RUN apt-get update && \
    apt-get install -y gcc libpq-dev && \
    rm -rf /var/lib/apt/lists/*

# Cài Python packages
RUN pip install --no-cache-dir -r requirements.txt

# Copy source code sau
COPY . .

# Run FastAPI
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "7003"]
