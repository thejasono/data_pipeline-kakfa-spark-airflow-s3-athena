FROM bitnamilegacy/spark:3.5.1

USER root

RUN set -eux; \
    apt-get update; \
    apt-get install -y --no-install-recommends \
        curl \
        python3 \
        python3-pip \
        python3-venv; \
    ln -s /usr/bin/python3 /usr/bin/python || true; \
    rm -rf /var/lib/apt/lists/*

# Still root here – create app dir and copy code
RUN mkdir -p /opt/spark/app
COPY app/ /opt/spark/app/

# Switch to non-root for the rest (spark default)
USER 1001

ARG KAFKA_CLIENT_VERSION=3.5.1
ARG SPARK_KAFKA_VERSION=3.5.1
ARG SCALA_BINARY_VERSION=2.12
ARG COMMONS_POOL_VERSION=2.12.0

RUN set -eux; \
    cd /opt/bitnami/spark/jars; \
    curl -fSLo kafka-clients-${KAFKA_CLIENT_VERSION}.jar \
      https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/${KAFKA_CLIENT_VERSION}/kafka-clients-${KAFKA_CLIENT_VERSION}.jar; \
    curl -fSLo spark-sql-kafka-0-10_${SCALA_BINARY_VERSION}-${SPARK_KAFKA_VERSION}.jar \
      https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_${SCALA_BINARY_VERSION}/${SPARK_KAFKA_VERSION}/spark-sql-kafka-0-10_${SCALA_BINARY_VERSION}-${SPARK_KAFKA_VERSION}.jar; \
    curl -fSLo spark-token-provider-kafka-0-10_${SCALA_BINARY_VERSION}-${SPARK_KAFKA_VERSION}.jar \
      https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_${SCALA_BINARY_VERSION}/${SPARK_KAFKA_VERSION}/spark-token-provider-kafka-0-10_${SCALA_BINARY_VERSION}-${SPARK_KAFKA_VERSION}.jar; \
    curl -fSLo commons-pool2-${COMMONS_POOL_VERSION}.jar \
      https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/${COMMONS_POOL_VERSION}/commons-pool2-${COMMONS_POOL_VERSION}.jar
