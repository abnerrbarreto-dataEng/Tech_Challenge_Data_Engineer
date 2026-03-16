ARG AIRFLOW_VERSION="2.8.1"
ARG PYTHON_VERSION="3.8"
FROM apache/airflow:${AIRFLOW_VERSION}-python${PYTHON_VERSION}

USER root

# Instala Java para PySpark e WGET
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre wget \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Instala os jars necessários para o Spark se conectar ao MinIO (S3a)
ARG HADOOP_VERSION="3.3.4"
ARG AWS_SDK_VERSION="1.12.672"

RUN mkdir -p /opt/spark/jars/ \
    && wget -q https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_VERSION}/hadoop-aws-${HADOOP_VERSION}.jar -P /opt/spark/jars/ \
    && wget -q https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_VERSION}/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar -P /opt/spark/jars/ \
    && wget -q https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-common/${HADOOP_VERSION}/hadoop-common-${HADOOP_VERSION}.jar -P /opt/spark/jars/ \
    && ls -l /opt/spark/jars/

# Cria o diretório de log para PySpark
RUN mkdir -p /opt/spark/logs

USER airflow

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt