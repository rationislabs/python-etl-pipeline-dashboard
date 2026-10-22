FROM python:3.12-slim@sha256:cb33f7f771ecec2ef1225cf39b159591a040391bd73e4c01f00fdfc4c47611d2
RUN pip install --no-cache-dir --upgrade pip==26.2.2 uv==0.13.5
ENV UV_NO_CACHE=1
WORKDIR /app
COPY pyproject.toml uv.lock ./
COPY etl/pyproject.toml etl/pyproject.toml
COPY dashboard/pyproject.toml dashboard/pyproject.toml
RUN uv export --frozen --package weather-dash --no-dev --no-emit-workspace -o /tmp/req.txt \
    && uv pip install --system -r /tmp/req.txt
COPY dashboard dashboard
RUN uv pip install --system --no-deps ./dashboard
ENV LAKEHOUSE=/lakehouse CITIES_FILE=/app/config/cities.yaml PYTHONUNBUFFERED=1
ENV HOME=/tmp
EXPOSE 8050
USER nobody
CMD ["gunicorn", "-b", "0.0.0.0:8050", "-w", "2", "weather_dash.app:server"]
