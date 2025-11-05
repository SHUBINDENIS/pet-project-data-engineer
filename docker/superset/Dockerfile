FROM apache/superset:3.1.0
USER root
RUN pip install --no-cache-dir \
    clickhouse-connect \
    pymongo \
    boto3
USER superset