FROM python:3.11

WORKDIR /frontend

# Chỉ copy requirements trước để tận dụng cache
COPY requirements.txt .

# Cài gói hệ thống
RUN apt-get update && apt-get install -y \
    build-essential \
    gcc \
    libffi-dev \
    libssl-dev \
    libjpeg-dev \
    zlib1g-dev \
    && rm -rf /var/lib/apt/lists/*

# Cài Python packages
RUN pip install --no-cache-dir -r requirements.txt

# Copy toàn bộ project vào sau
COPY . .

EXPOSE 8000

CMD ["python", "app.py"]

