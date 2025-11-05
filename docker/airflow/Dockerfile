FROM apache/airflow:2.9.3-python3.12
USER root
RUN apt-get update && \
    apt-get install -y --no-install-recommends gcc g++ build-essential && \
    rm -rf /var/lib/apt/lists/*
USER airflow
RUN pip install --no-cache-dir \
    apache-airflow-providers-amazon==9.4.0 \
    apache-airflow-providers-http==5.4.0 \
    apache-airflow-providers-mongo==3.4.0 \
    apache-airflow-providers-apache-spark==4.3.0 \
    boto3 \
    clickhouse-connect \
    pymongo \
    pandas \
    pyarrow \
    requests \
    binance-connector \
    fredapi \
    textblob \
    vaderSentiment
RUN python -m textblob.download_corpora || true
