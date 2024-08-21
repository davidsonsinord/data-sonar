import boto3
from botocore.client import Config
import pandas as pd
from sqlalchemy import create_engine
from io import TextIOWrapper
import sys

print(sys.argv)

_endpoint_url = sys.argv[1]
_access_key_id = sys.argv[2]
_secret_access_key = sys.argv[3]
_bucket_name = sys.argv[4]
_filename = sys.argv[5]
_chunk_size = sys.argv[6]
_separator = sys.argv[7]
_username = sys.argv[8]
_password = sys.argv[9]
_hostname = sys.argv[10]
_port = sys.argv[11]
_database = sys.argv[12]
_schema_name = sys.argv[13]

# Init S3 client using boto3
s3 = boto3.client('s3',
                  endpoint_url=_endpoint_url,
                  aws_access_key_id=_access_key_id,
                  aws_secret_access_key=_secret_access_key,
                  config=Config(signature_version='s3v4'),
                  verify=False)

try:
    # Init postresql engine using sqlalchemy + psycopg2
    engine = create_engine('postgresql+psycopg2://'+_username+':'+_password+'@'+_hostname+':'+_port+'/'+_database)

    response = s3.get_object(Bucket=_bucket_name, Key=_filename)
    streaming_body = response['Body']
    chunk_size = int(_chunk_size)
    table_name = _filename.rsplit('/', 1)[1].rsplit('.', 1)[0]

    # Utiliser TextIOWrapper pour lire en chunks
    def stream_csv_chunks(streaming_body, chunk_size):
        with TextIOWrapper(streaming_body, encoding='utf-8') as text_stream:
            reader = pd.read_csv(text_stream, chunksize=chunk_size, sep=_separator)
            for chunk in reader:
                yield chunk

    # Insérer chaque chunk dans PostgreSQL
    for chunk in stream_csv_chunks(streaming_body, chunk_size):
        chunk.to_sql(table_name, engine, if_exists='append', index=False, schema=_schema_name)

    # Fermeture de la connexion PostgreSQL
    engine.dispose()

except Exception as e:
    print(f"Erreur lors de l'ouverture du fichier : {e}")