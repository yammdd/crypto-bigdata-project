FROM apache/spark:3.4.1

USER root

RUN mkdir -p /opt/spark/jars

# Spark Kafka connector
ADD https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.4.1/spark-sql-kafka-0-10_2.12-3.4.1.jar /opt/spark/jars/
ADD https://repo1.maven.org/maven2/org/apache/spark/spark-streaming-kafka-0-10-assembly_2.12/3.4.1/spark-streaming-kafka-0-10-assembly_2.12-3.4.1.jar /opt/spark/jars/
ADD https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.4.0/kafka-clients-3.4.0.jar /opt/spark/jars/

# Thư viện bị thiếu cho KafkaDataConsumer
ADD https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar /opt/spark/jars/

ENV SPARK_EXTRA_CLASSPATH="/opt/spark/jars/*"
