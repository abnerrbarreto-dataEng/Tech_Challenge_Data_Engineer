import argparse
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, coalesce, lit, count

# --- Configuração de Caminhos ---
BASE_PATH = "s3a://brewery-datalake"

def create_spark_session():
    """
    Cria e retorna uma SparkSession configurada para se conectar ao MinIO.
    As configurações do MinIO são lidas das variáveis de ambiente.
    """
    # Lendo variáveis de ambiente para configuração do MinIO
    minio_endpoint = os.environ.get("MINIO_ENDPOINT", "http://minio:9000") # Default para ambiente Docker
    minio_access_key = os.environ.get("MINIO_ACCESS_KEY", "minioadmin")
    minio_secret_key = os.environ.get("MINIO_SECRET_KEY", "minioadmin")

    return SparkSession.builder \
        .appName("BrewerySilverLayer") \
        .config("spark.driver.extraJavaOptions", "-Dcom.amazonaws.services.s3.enableV4=true") \
        .config("spark.executor.extraJavaOptions", "-Dcom.amazonaws.services.s3.enableV4=true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.endpoint", minio_endpoint) \
        .config("spark.hadoop.fs.s3a.access.key", minio_access_key) \
        .config("spark.hadoop.fs.s3a.secret.key", minio_secret_key) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.fast.upload", "true") \
        .config("spark.hadoop.fs.s3a.multiobjectdelete.enable", "true") \
        .config("spark.hadoop.fs.s3a.multipart.size", "10485760") \
        .config("spark.hadoop.fs.s3a.committer.name", "directory") \
        .config("spark.hadoop.fs.s3a.committer.magic.enabled", "false") \
        .config("spark.sql.shuffle.partitions", "8") \
        .getOrCreate()

def transform_to_silver(spark, execution_date):
    bronze_input_path = f"{BASE_PATH}/bronze/breweries/{execution_date}/breweries_raw.json"
    silver_output_path = f"{BASE_PATH}/silver/breweries/{execution_date}"

    print(f"Silver: Lendo dados da Bronze em: {bronze_input_path}")

    try:
        df_bronze = spark.read.option("multiLine", "true").json(bronze_input_path)
        
        if df_bronze.rdd.isEmpty():
            print("Silver: Nenhum dado encontrado na Bronze para processar. Finalizando Silver.")
            return
        
        print("Silver: Schema inferido da Bronze:")
        df_bronze.printSchema()
        print("Silver: Amostra dos dados da Bronze (id, name, state_province, state):")
        df_bronze.select("id", "name", "state_province", "state").show(5, truncate=False)

        # Trata state_province nulo, usando 'state' como fallback e 'Unknown' se ambos forem nulos.
        df_silver = df_bronze \
            .withColumn("state_province", coalesce(col("state_province"), col("state"), lit("Unknown"))) \
            .dropDuplicates(['id'])
        
        # --- Debug Adicional ---
        total_rows = df_silver.count()
        null_state_province_rows = df_silver.filter(col("state_province").isNull()).count()
        unknown_state_province_rows = df_silver.filter(col("state_province") == "Unknown").count()
        
        print(f"Silver: Total de linhas após deduplicação: {total_rows}")
        print(f"Silver: Linhas com state_province NULO após coalesce: {null_state_province_rows}")
        print(f"Silver: Linhas com state_province 'Unknown' após coalesce: {unknown_state_province_rows}")
        
        if total_rows == 0:
            print("Silver: Nenhum dado restante para escrever após transformações.")
            return

        print(f"Silver: Escrevendo dados particionados em: {silver_output_path}")

        df_silver.write \
            .mode("overwrite") \
            .partitionBy("state_province") \
            .parquet(silver_output_path)

        print("Silver: Sucesso! Camada Silver gerada.")

    except Exception as e:
        print(f"Silver: Erro ao processar a camada Bronze para Silver para a data {execution_date}: {e}")
        raise

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process data from Bronze to Silver layer using PySpark.")
    parser.add_argument("--execution_date", help="The logical execution date in YYYY-MM-DD format.")
    args = parser.parse_args()

    if not args.execution_date:
        args.execution_date = datetime.now().strftime("%Y-%m-%d")


    spark = create_spark_session()
    transform_to_silver(spark, args.execution_date)
    spark.stop()
