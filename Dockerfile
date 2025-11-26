version: '3.8'
services:
  etl:
    build:
      context: .
      dockerfile: etl/Dockerfile
    ports:
      - "8080:8080"
    volumes:
      - ./data:/app/data
      - ./output:/app/output
      - ./credentials.json:/app/credentials.json:ro
    environment:
      - AIRFLOW__CORE__EXECUTOR=LocalExecutor
      - AIRFLOW__CORE__DAGS_FOLDER=/app/airflow_dags
  dashboard:
    build:
      context: .
      dockerfile: dashboard/Dockerfile
    ports:
      - "8050:8050"
    volumes:
      - ./output:/app/output
      - ./credentials.json:/app/credentials.json:ro
    depends_on:
      - etl