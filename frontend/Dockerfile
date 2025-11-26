FROM python:3.11

WORKDIR /app

COPY . .

# Cài đặt các thư viện cần thiết cho việc biên dịch (ví dụ: scikit-learn cần gcc)
# và thư viện cho kết nối PostgreSQL (libpq-dev)
RUN apt-get update && \
    apt-get install -y gcc libpq-dev && \
    rm -rf /var/lib/apt/lists/*

# Cài đặt các dependencies Python.
# FastAPI và Uvicorn cho API, pandas và sqlalchemy cho xử lý dữ liệu/DB, scikit-learn cho clustering.
RUN pip install --no-cache-dir fastapi uvicorn pandas sqlalchemy scikit-learn

# Khởi chạy FastAPI app
# main:app là tham chiếu đến instance FastAPI trong file main.py
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "7003"]