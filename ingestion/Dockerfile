# ingestion/Dockerfile
FROM python:3.11

WORKDIR /app

COPY . .

RUN apt-get update && \
    apt-get install -y gcc libpq-dev && \
    rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir pandas requests sqlalchemy psycopg2-binary vnstock3

CMD ["python", "ingestion.py"]
