FROM apache/airflow:2.9.3-python3.12

USER root
RUN apt-get update && \
    apt-get install -y --no-install-recommends gcc g++ build-essential && \
    rm -rf /var/lib/apt/lists/*

USER airflow
ARG AIRFLOW_VERSION=2.9.3
ARG PYTHON_VERSION=3.12
ARG CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

RUN pip install --no-cache-dir \
    apache-airflow-providers-amazon \
    apache-airflow-providers-http \
    apache-airflow-providers-mongo \
    apache-airflow-providers-apache-spark \
    boto3 \
    clickhouse-connect \
    pymongo \
    pandas \
    pyarrow \
    requests \
    binance-connector \
    fredapi \
    textblob \
    vaderSentiment \
    --constraint "${CONSTRAINT_URL}"

RUN python -m textblob.download_corpora || true
