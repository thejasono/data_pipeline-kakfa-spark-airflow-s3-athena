# syntax=docker/dockerfile:1
FROM bitnami/spark:3.5.1

USER root

RUN set -eux; \
    apt-get update; \
    apt-get install -y --no-install-recommends curl python3; \
    rm -rf /var/lib/apt/lists/*

USER 1001

# Keep connector artifacts aligned with the Spark distribution that Bitnami ships
# (Spark 3.5.1 built against Scala 2.12 and Kafka 3.5.x). Mixing major versions
# causes class-loading errors at runtime, which previously stopped the streaming
# job before it could write to S3.
ARG KAFKA_CLIENT_VERSION=3.5.1
ARG SPARK_KAFKA_VERSION=3.5.1
ARG SCALA_BINARY_VERSION=2.12
ARG HADOOP_AWS_VERSION=3.3.4
ARG AWS_SDK_BUNDLE_VERSION=1.12.262
ARG COMMONS_POOL_VERSION=2.12.0

# Pre-download the connector JARs that the Spark Structured Streaming job
# expects so operators do not have to fetch them manually in the running
# container.
RUN set -eux; \
    cd /opt/bitnami/spark/jars; \
    curl -fSLo kafka-clients-${KAFKA_CLIENT_VERSION}.jar \
      https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/${KAFKA_CLIENT_VERSION}/kafka-clients-${KAFKA_CLIENT_VERSION}.jar; \
    curl -fSLo spark-sql-kafka-0-10_${SCALA_BINARY_VERSION}-${SPARK_KAFKA_VERSION}.jar \
      https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_${SCALA_BINARY_VERSION}/${SPARK_KAFKA_VERSION}/spark-sql-kafka-0-10_${SCALA_BINARY_VERSION}-${SPARK_KAFKA_VERSION}.jar; \
    curl -fSLo spark-token-provider-kafka-0-10_${SCALA_BINARY_VERSION}-${SPARK_KAFKA_VERSION}.jar \
      https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_${SCALA_BINARY_VERSION}/${SPARK_KAFKA_VERSION}/spark-token-provider-kafka-0-10_${SCALA_BINARY_VERSION}-${SPARK_KAFKA_VERSION}.jar; \
    curl -fSLo hadoop-aws-${HADOOP_AWS_VERSION}.jar \
      https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar; \
    curl -fSLo aws-java-sdk-bundle-${AWS_SDK_BUNDLE_VERSION}.jar \
      https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_BUNDLE_VERSION}/aws-java-sdk-bundle-${AWS_SDK_BUNDLE_VERSION}.jar; \
    curl -fSLo commons-pool2-${COMMONS_POOL_VERSION}.jar \
      https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/${COMMONS_POOL_VERSION}/commons-pool2-${COMMONS_POOL_VERSION}.jar
