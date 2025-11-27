# backend/prediction/Dockerfile
FROM python:3.11

WORKDIR /app

# Cài hệ thống cần thiết
RUN apt-get update && \
    apt-get install -y gcc libpq-dev && \
    rm -rf /var/lib/apt/lists/*

# Copy requirements trước để cache layer install
COPY requirements.txt .

# Cài Python packages
RUN pip install --no-cache-dir -r requirements.txt

# Copy toàn bộ source code sau
COPY . .

# Expose và chạy server
EXPOSE 7002
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "7002"]
