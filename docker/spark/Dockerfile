FROM apache/spark:3.4.1

USER root

RUN mkdir -p /opt/spark/jars
RUN apt-get update && apt-get install -y software-properties-common && \
    add-apt-repository ppa:deadsnakes/ppa && \
    apt-get update && \
    apt-get install -y python3.9 python3.9-distutils curl && \
    ln -sf /usr/bin/python3.9 /usr/bin/python3 && \
    curl -sS https://bootstrap.pypa.io/get-pip.py | python3.9

RUN apt-get update && apt-get install -y curl openjdk-8-jdk
RUN curl -O https://downloads.apache.org/hadoop/common/hadoop-3.3.6/hadoop-3.3.6.tar.gz && \
    tar -xzf hadoop-3.3.6.tar.gz && \
    mv hadoop-3.3.6 /opt/hadoop && \
    ln -s /opt/hadoop/bin/hadoop /usr/bin/hadoop

ADD https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.4.1/spark-sql-kafka-0-10_2.12-3.4.1.jar /opt/spark/jars/
ADD https://repo1.maven.org/maven2/org/apache/spark/spark-streaming-kafka-0-10-assembly_2.12/3.4.1/spark-streaming-kafka-0-10-assembly_2.12-3.4.1.jar /opt/spark/jars/
ADD https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.4.0/kafka-clients-3.4.0.jar /opt/spark/jars/
ADD https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar /opt/spark/jars/

ENV SPARK_EXTRA_CLASSPATH="/opt/spark/jars/*"
ENV PYTHONPATH="/opt/spark/work-dir"
ENV XGBOOST_VERSION=1.6.1
ENV SCALA_VERSION=2.12
ENV SPARK_VERSION=3.4.1

RUN wget https://repo1.maven.org/maven2/ml/dmlc/xgboost4j-spark_${SCALA_VERSION}/${XGBOOST_VERSION}/xgboost4j-spark_${SCALA_VERSION}-${XGBOOST_VERSION}.jar -P /opt/spark/jars/ && \
    wget https://repo1.maven.org/maven2/ml/dmlc/xgboost4j_${SCALA_VERSION}/${XGBOOST_VERSION}/xgboost4j_${SCALA_VERSION}-${XGBOOST_VERSION}.jar -P /opt/spark/jars/

RUN pip3 install happybase thrift yfinance xgboost pandas numpy scikit-learn joblib pymongo streamlit plotly matplotlib python-dateutil pyspark google-generativeai newsapi-python

WORKDIR /opt/spark/work-dir

COPY ../../streaming /opt/spark/work-dir/streaming
COPY ../../batch /opt/spark/work-dir/batch
