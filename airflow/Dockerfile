FROM base
USER root
ENV AIRFLOW_HOME=/opt/airflow \
    PYTHONPATH=/opt/airflow/dags
RUN uv pip install --system "apache-airflow[postgres]==3.3.2" pytest \
      --constraint https://raw.githubusercontent.com/apache/airflow/constraints-3.3.2/constraints-3.12.txt \
    && mkdir -p /opt/airflow/dags /opt/airflow/tests /opt/airflow/logs \
    && chown -R app /opt/airflow
COPY --chown=app airflow/dags /opt/airflow/dags
COPY --chown=app airflow/tests /opt/airflow/tests
USER app
