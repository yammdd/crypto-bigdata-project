FROM apache/airflow:2.9.1-python3.10

USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential \
        python3-dev \
    && rm -rf /var/lib/apt/lists/*

USER airflow
RUN pip install --no-cache-dir yfinance kafka-python pandas requests