# backend/prediction/Dockerfile
FROM python:3.11

WORKDIR /app

COPY . .

RUN apt-get update && \
    apt-get install -y gcc libpq-dev && \
    rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir fastapi uvicorn pandas sqlalchemy scikit-learn psycopg2-binary

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "7002"]
