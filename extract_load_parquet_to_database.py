import pandas as pd
import boto3
import io
from sqlalchemy import create_engine
import pyarrow
from dotenv import load_dotenv
import os

load_dotenv()

engine = create_engine(os.getenv("URL_SUPABASE"))

print(engine)

print("Acessando cliente s3... ✅")

s3 = boto3.client(
    "s3",
    region_name=os.getenv("AWS_REGION"),
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),

)

BUCKET_NAME = os.getenv("BUCKET_NAME")
#Listar arquivos no bucket
print(f"Buscando arquivos parquet no bucket {BUCKET_NAME} ✅")

response = s3.list_objects(Bucket=BUCKET_NAME)

for obj in response["Contents"]:
  key = obj["Key"]

  if key.endswith(".parquet"):
    print(f"Arquivo encontrado {key} ✅")
    response = s3.get_object(Bucket=os.getenv("BUCKET_NAME"), Key=key)
    parquet_bytes = response["Body"].read()
    parquet = io.BytesIO(parquet_bytes)

    df = pd.read_parquet(parquet)

    tabela = key.replace(".parquet", "")

    df.to_sql(
      tabela,
      engine,
      index=False,
      if_exists="replace"
  )

    print(f"Tabela {tabela} criada com sucesso! ✅")


