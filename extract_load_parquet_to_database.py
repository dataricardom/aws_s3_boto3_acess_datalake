import pandas as pd
import boto3
import io
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os
import logging

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),                      # continua no terminal
        logging.FileHandler("pipeline.log")           # salva em arquivo também
    ]
)
log = logging.getLogger(__name__)

engine = create_engine(os.getenv("URL_SUPABASE"))
BUCKET_NAME = os.getenv("BUCKET_NAME")


def acesso_s3_client():
    log.info("Acessando cliente s3... ✅")
    return boto3.client(
        "s3",
        region_name=os.getenv("AWS_REGION"),
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    )


def listar_arquivos(s3_client):
    log.info(f"Buscando arquivos parquet no bucket {BUCKET_NAME} ✅")
    paginator = s3_client.get_paginator("list_objects_v2")
    arquivos = []
    for page in paginator.paginate(Bucket=BUCKET_NAME):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.endswith(".parquet"):
                arquivos.append(key)
    return arquivos  


def carregar_parquet(s3_client, key):  
    log.info(f"Lendo arquivo {key} ✅")
    response = s3_client.get_object(Bucket=BUCKET_NAME, Key=key)
    parquet_bytes = response["Body"].read()
    df = pd.read_parquet(io.BytesIO(parquet_bytes))
    return df


def salvar_tabela(df, tabela):
    try:  # ✅ BUG 4 corrigido
        df.to_sql(tabela, engine, index=False, if_exists="replace")
        log.info(f"Tabela {tabela} criada com sucesso! {len(df)} registros inseridos ✅")
    except Exception as e:
        log.error(f"Erro ao carregar tabela {tabela}: {e}")


def main():
    log.info("Iniciando pipeline ✅")
    s3 = acesso_s3_client() 
    arquivos = listar_arquivos(s3)
    for arquivo in arquivos:
        df = carregar_parquet(s3, arquivo)
        tabela = os.path.basename(arquivo).replace(".parquet", "")
        salvar_tabela(df, tabela)
    log.info("Pipeline Finalizado ✅")


if __name__ == "__main__":
    main()