FROM apache/spark:3.4.1

USER root

RUN mkdir -p /opt/spark/jars
RUN apt-get update && apt-get install -y software-properties-common && \
    add-apt-repository ppa:deadsnakes/ppa && \
    apt-get update && \
    apt-get install -y python3.9 python3.9-distutils curl && \
    ln -sf /usr/bin/python3.9 /usr/bin/python3 && \
    curl -sS https://bootstrap.pypa.io/get-pip.py | python3.9

# Spark Kafka connectors
ADD https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.4.1/spark-sql-kafka-0-10_2.12-3.4.1.jar /opt/spark/jars/
ADD https://repo1.maven.org/maven2/org/apache/spark/spark-streaming-kafka-0-10-assembly_2.12/3.4.1/spark-streaming-kafka-0-10-assembly_2.12-3.4.1.jar /opt/spark/jars/
ADD https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.4.0/kafka-clients-3.4.0.jar /opt/spark/jars/
ADD https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar /opt/spark/jars/

ENV SPARK_EXTRA_CLASSPATH="/opt/spark/jars/*"
ENV PYTHONPATH="/opt/spark/work-dir"

RUN pip3 install happybase thrift yfinance

WORKDIR /opt/spark/work-dir

COPY ../../streaming /opt/spark/work-dir/streaming
COPY ../../batch /opt/spark/work-dir/batch