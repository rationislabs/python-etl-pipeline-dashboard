FROM apache/airflow:2.10.2-python3.11
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY src /app
COPY data /app/data
COPY output /app/output
ENV AIRFLOW__CORE__DAGS_FOLDER=/app/airflow_dags
CMD ["airflow", "standalone"]