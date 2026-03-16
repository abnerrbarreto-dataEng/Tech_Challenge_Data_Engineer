#!/bin/bash

PYSPARK_SCRIPT=$1
EXECUTION_DATE=$2

echo "--- Iniciando script PySpark: ${PYSPARK_SCRIPT} para a data: ${EXECUTION_DATE} ---"

if [ ! -f "${PYSPARK_SCRIPT}" ]; then
  echo "Erro: Script PySpark ${PYSPARK_SCRIPT} não encontrado!"
  exit 1
fi

MINIO_ENDPOINT=${MINIO_ENDPOINT}
MINIO_ACCESS_KEY=${MINIO_ACCESS_KEY}
MINIO_SECRET_KEY=${MINIO_SECRET_KEY}

HADOOP_VERSION_FOR_JARS="3.3.4"
AWS_SDK_VERSION_FOR_JARS="1.12.672"

SPARK_JARS_DIR="/opt/spark/jars"
HADOOP_AWS_JAR="${SPARK_JARS_DIR}/hadoop-aws-${HADOOP_VERSION_FOR_JARS}.jar"
AWS_SDK_BUNDLE_JAR="${SPARK_JARS_DIR}/aws-java-sdk-bundle-${AWS_SDK_VERSION_FOR_JARS}.jar"
HADOOP_COMMON_JAR="${SPARK_JARS_DIR}/hadoop-common-${HADOOP_VERSION_FOR_JARS}.jar" 

# Verifica se os JARs realmente existem.
if [ ! -f "${HADOOP_AWS_JAR}" ] || [ ! -f "${AWS_SDK_BUNDLE_JAR}" ] || [ ! -f "${HADOOP_COMMON_JAR}" ]; then
  echo "Erro: Um ou mais JARs do Hadoop S3A, AWS SDK ou Hadoop Common não encontrados em ${SPARK_JARS_DIR}."
  echo "Conteúdo de ${SPARK_JARS_DIR}:"
  ls -l ${SPARK_JARS_DIR} 
  exit 1
fi

JARS_ARG="${HADOOP_AWS_JAR},${AWS_SDK_BUNDLE_JAR},${HADOOP_COMMON_JAR}" 

echo "Spark-Submit: Adicionando JARs ao classpath: ${JARS_ARG}" 

spark-submit \
  --master local[*] \
  --jars "${JARS_ARG}" \
  --conf "spark.driver.extraJavaOptions=-Dcom.amazonaws.services.s3.enableV4=true" \
  --conf "spark.executor.extraJavaOptions=-Dcom.amazonaws.services.s3.enableV4=true" \
  --conf "spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem" \
  --conf "spark.hadoop.fs.s3a.endpoint=${MINIO_ENDPOINT}" \
  --conf "spark.hadoop.fs.s3a.access.key=${MINIO_ACCESS_KEY}" \
  --conf "spark.hadoop.fs.s3a.secret.key=${MINIO_SECRET_KEY}" \
  --conf "spark.hadoop.fs.s3a.path.style.access=true" \
  --conf "spark.hadoop.fs.s3a.fast.upload=true" \
  --conf "spark.hadoop.fs.s3a.multiobjectdelete.enable=true" \
  --conf "spark.hadoop.fs.s3a.multipart.size=10485760" \
  --conf "spark.hadoop.fs.s3a.committer.name=directory" \
  --conf "spark.hadoop.fs.s3a.committer.magic.enabled=false" \
  "${PYSPARK_SCRIPT}" \
  --execution_date "${EXECUTION_DATE}"

if [ $? -eq 0 ]; then
  echo "--- Script PySpark ${PYSPARK_SCRIPT} concluído com sucesso. ---"
else
  echo "--- Script PySpark ${PYSPARK_SCRIPT} falhou. ---"
  exit 1
fi