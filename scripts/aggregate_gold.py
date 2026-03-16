import argparse
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import count, col

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
        .appName("BreweryGoldLayer") \
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
        .config("spark.sql.shuffle.partitions", "4") \
        .getOrCreate()

def aggregate_to_gold(spark, execution_date):
    silver_input_path = f"{BASE_PATH}/silver/breweries/{execution_date}"
    gold_output_path = f"{BASE_PATH}/gold/brewery_summary/{execution_date}"

    print(f"Gold: Lendo dados da Silver em: {silver_input_path}")

    try:
        # Tentar ler os dados da Silver.
        df_silver = spark.read.parquet(silver_input_path)

        if df_silver.rdd.isEmpty():
            print(f"Gold: Nenhum dado encontrado na Silver para a data {execution_date}. Pulando a agregação para Gold.")
            return
            
        print("Gold: Schema da Silver:")
        df_silver.printSchema()
        print("Gold: Amostra dos dados da Silver (state_province, brewery_type):")
        df_silver.select("state_province", "brewery_type").show(5, truncate=False)

        # 1. Agregação: Quantidade de cervejarias por tipo e localização (state_province)
        df_gold = df_silver.groupBy("brewery_type", "state_province") \
                           .agg(count("id").alias("qtd_breweries")) \
                           .orderBy("state_province", "brewery_type")

        print("Gold: Amostra dos dados agregados (Gold):")
        df_gold.show(10, truncate=False)

        # Salvar na Camada Gold como Parquet no MinIO
        df_gold.write \
            .mode("overwrite") \
            .parquet(gold_output_path)

        print(f"Gold: Sucesso! Agregação salva na Gold para a data: {execution_date}")

    except Exception as e:
        print(f"Gold: Erro ao processar a camada Silver para Gold para a data {execution_date}: {e}")
        raise 

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Aggregate data from Silver to Gold layer using PySpark.")
    parser.add_argument("--execution_date", help="The logical execution date in YYYY-MM-DD format.")
    args = parser.parse_args()

    if not args.execution_date:
        args.execution_date = datetime.now().strftime("%Y-%m-%d")
            
    spark = create_spark_session()
    aggregate_to_gold(spark, args.execution_date)
    spark.stop()