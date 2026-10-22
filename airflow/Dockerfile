FROM base
USER root
ENV AIRFLOW_HOME=/opt/airflow \
    PYTHONPATH=/opt/airflow/dags
COPY airflow/overrides.txt /tmp/overrides.txt
# Airflow's constraints, minus the pins overrides.txt raises (a constraint wins over an override).
RUN curl -fsSL https://raw.githubusercontent.com/apache/airflow/constraints-3.3.2/constraints-3.12.txt \
      | grep -viE "^($(grep -oE '^[A-Za-z0-9_.-]+' /tmp/overrides.txt | paste -sd'|'))==" > /tmp/constraints.txt \
    && uv pip install --system "apache-airflow[postgres]==3.3.2" pytest \
      --constraint /tmp/constraints.txt --constraint /tmp/overrides.txt \
    && mkdir -p /opt/airflow/dags /opt/airflow/tests /opt/airflow/logs \
    && chown -R app /opt/airflow
COPY --chown=app airflow/dags /opt/airflow/dags
COPY --chown=app airflow/tests /opt/airflow/tests
USER app
