import boto3
from botocore.client import Config
import pandas as pd
from sqlalchemy import create_engine
from io import TextIOWrapper

# Init S3 client using boto3
s3 = boto3.client('s3',
                  endpoint_url='{{inputs.s3.endpoint_url}}',
                  aws_access_key_id='{{inputs.s3.access_key_id}}',
                  aws_secret_access_key='{{inputs.s3.secret_access_key}}',
                  config=Config(signature_version='s3v4'),
                  verify=False)

try:
    # Init postresql engine using sqlalchemy + psycopg2
    engine = create_engine('postgresql+psycopg2://{{inputs.postgresql.username}}:{{inputs.postgresql.password}}@{{inputs.postgresql.hostname}}:{{inputs.postgresql.port}}/{{inputs.postgresql.database}}')

    response = s3.get_object(Bucket='{{inputs.s3.bucket_name}}', Key='{{inputs.s3.filename}}')
    streaming_body = response['Body']
    chunk_size = {{inputs.s3.chunk_size}}
    table_name = '{{inputs.s3.filename}}'.rsplit('/', 1)[1].rsplit('.', 1)[0]

    # Utiliser TextIOWrapper pour lire en chunks
    def stream_csv_chunks(streaming_body, chunk_size):
        with TextIOWrapper(streaming_body, encoding='utf-8') as text_stream:
            reader = pd.read_csv(text_stream, chunksize=chunk_size)
            for chunk in reader:
                yield chunk

    # Insérer chaque chunk dans PostgreSQL
    for chunk in stream_csv_chunks(streaming_body, chunk_size):
        chunk.to_sql(table_name, engine, if_exists='append', index=False, schema='{{inputs.postgresql.schema_name}}')

    # Fermeture de la connexion PostgreSQL
    engine.dispose()

except Exception as e:
    print(f"Erreur lors de l'ouverture du fichier : {e}")