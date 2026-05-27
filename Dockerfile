FROM apache/airflow:2.10.0

USER airflow

RUN pip install --no-cache-dir \
    dbt-core==1.10.9 \
    dbt-snowflake==1.10.2


