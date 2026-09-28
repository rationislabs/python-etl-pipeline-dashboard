# Shared runtime for Spark master/workers and the Airflow image: identical Python, Java, PySpark
# and weather_etl everywhere, so driver and executors can never disagree on versions.
FROM python:3.12-slim

ARG LOCAL_UID=1000
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-21-jre-headless procps curl \
    && rm -rf /var/lib/apt/lists/* \
    && ln -s "$(dirname "$(dirname "$(readlink -f "$(command -v java)")")")" /opt/java \
    && useradd --create-home --uid "${LOCAL_UID}" app

ENV JAVA_HOME=/opt/java \
    SPARK_HOME=/usr/local/lib/python3.12/site-packages/pyspark \
    SPARK_IVY_DIR=/opt/ivy \
    SPARK_WORKER_DIR=/tmp/spark-work \
    SPARK_LOCAL_DIRS=/tmp/spark-local \
    LAKEHOUSE=/lakehouse \
    CITIES_FILE=/app/config/cities.yaml \
    PYTHONUNBUFFERED=1

RUN pip install --no-cache-dir uv==0.11.25
ENV UV_NO_CACHE=1
WORKDIR /app
COPY pyproject.toml uv.lock ./
COPY etl/pyproject.toml etl/pyproject.toml
COPY dashboard/pyproject.toml dashboard/pyproject.toml
RUN uv export --frozen --package weather-etl --no-dev --no-emit-workspace --no-hashes -o /tmp/req.txt \
    && uv pip install --system -r /tmp/req.txt
COPY etl etl
RUN uv pip install --system --no-deps ./etl
COPY config config

RUN mkdir -p /opt/ivy /lakehouse && chown -R app /opt/ivy /lakehouse
USER app
# Pre-fetch Delta jars so containers never need Maven at runtime.
RUN python -c "from weather_etl.spark import get_session; get_session('warmup', master='local[1]').stop()"
