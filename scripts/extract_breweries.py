import io
import json
import os
from datetime import datetime
import argparse

import requests
from minio import Minio
from minio.error import S3Error

# --- Configurações ---
API_BASE_URL = "https://api.openbrewerydb.org/v1/breweries"
HEADERS = {
    "User-Agent": "BreweryDataPipeline/1.0 (contact@example.com)"
}

# Configurações do MinIO
MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "minio:9000").replace("http://", "")
MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY", "minioadmin")
BUCKET_NAME = "brewery-datalake"

def create_minio_client():
    return Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False
    )

def ensure_bucket_exists(client, bucket_name):
    try:
        if not client.bucket_exists(bucket_name):
            client.make_bucket(bucket_name)
            print(f"Bucket '{bucket_name}' criado com sucesso no MinIO.")
        else:
            print(f"Bucket '{bucket_name}' já existe no MinIO.")
    except S3Error as e:
        print(f"Erro ao verificar/criar bucket '{bucket_name}': {e}")
        raise

def fetch_all_breweries():
    all_breweries = []
    page = 1
    per_page = 50

    print(f"Iniciando a busca de cervejarias da API: {API_BASE_URL}")

    while True:
        params = { "page": page, "per_page": per_page }
        try:
            response = requests.get(API_BASE_URL, headers=HEADERS, params=params)
            response.raise_for_status()
            current_page_data = response.json()
            if not current_page_data:
                print(f"Página {page} vazia. Todas as cervejarias foram buscadas.")
                break
            all_breweries.extend(current_page_data)
            print(f"Página {page} buscada. Total de cervejarias até agora: {len(all_breweries)}")
            page += 1
        except requests.exceptions.HTTPError as e:
            print(f"Erro HTTP ao buscar dados da página {page}: {e}")
            raise
        except requests.exceptions.RequestException as e:
            print(f"Erro de conexão ao buscar dados da página {page}: {e}")
            raise
        except json.JSONDecodeError as e:
            print(f"Erro ao decodificar JSON da página {page}: {e}")
            raise
    print(f"Busca completa. Total de cervejarias encontradas: {len(all_breweries)}")
    return all_breweries

REQUIRED_FIELDS = [
    "id",
    "name",
    "brewery_type",
]


def validate_records(records):
    """Validate a list of brewery records.

    This performs basic integrity checks before writing data to storage.
    """
    if not isinstance(records, list):
        raise ValueError("Data must be a list of brewery records.")

    for i, record in enumerate(records, start=1):
        if not isinstance(record, dict):
            raise ValueError(f"Record {i} is not an object: {record}")
        missing = [f for f in REQUIRED_FIELDS if f not in record or record.get(f) in (None, "")]
        if missing:
            raise ValueError(f"Record {i} is missing required fields: {missing}")


def save_raw_data_to_s3(client, bucket_name, object_name, data):
    """Salva os dados como JSON no MinIO (S3)."""
    validate_records(data)

    json_data = json.dumps(data, ensure_ascii=False, indent=4).encode('utf-8')
    try:
        client.put_object(
            bucket_name,
            object_name,
            data=io.BytesIO(json_data),
            length=len(json_data),
            content_type="application/json"
        )
        print(f"Dados brutos salvos em s3a://{bucket_name}/{object_name}")
    except S3Error as e:
        print(f"Erro ao salvar dados no MinIO: {e}")
        raise

if __name__ == "__main__":
    import io # Precisa importar aqui para save_raw_data_to_s3

    parser = argparse.ArgumentParser(description="Extract brewery data from API.")
    parser.add_argument("--execution_date", help="The logical execution date in YYYY-MM-DD format.")
    args = parser.parse_args()

    if not args.execution_date:
        args.execution_date = datetime.now().strftime("%Y-%m-%d")

    print(f"Iniciando script de extração em: {datetime.now()}")
    
    # 1. Conecta ao MinIO e garante que o bucket exista
    minio_client = create_minio_client()
    ensure_bucket_exists(minio_client, BUCKET_NAME)

    # 2. Busca os dados
    breweries_data = fetch_all_breweries()
    
    # 3. Define o caminho no S3
    s3_path = f"bronze/breweries/{args.execution_date}/breweries_raw.json"

    if breweries_data:
        save_raw_data_to_s3(minio_client, BUCKET_NAME, s3_path, breweries_data)
    else:
        print("Nenhum dado de cervejaria para salvar.")
    
    print(f"Script de extração finalizado em: {datetime.now()}")
