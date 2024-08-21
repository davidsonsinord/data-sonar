import boto3
from botocore.client import Config
import pandas as pd
from io import StringIO
from sqlalchemy import create_engine

# Init S3 client using boto3
s3 = boto3.client('s3',
                  endpoint_url='{{inputs.s3.endpoint_url}}',
                  aws_access_key_id='{{inputs.s3.access_key_id}}',
                  aws_secret_access_key='{{inputs.s3.secret_access_key}}',
                  config=Config(signature_version='s3v4'),
                  verify=False)

try:
  response = s3.get_object(Bucket='{{inputs.s3.bucket_name}}', Key='{{inputs.s3.filename}}')
  file_content = response['Body'].read().decode('utf-8')
  csv_data = StringIO(file_content)
  df = pd.read_csv(csv_data, sep='{{inputs.s3.csv_separator}}')

  # Store csv data as Kestra output, for debugging purpose
  df.to_csv("out.csv", index = False)

  # Init postresql engine using sqlalchemy + psycopg2
  engine = create_engine('postgresql+psycopg2://{{inputs.postgresql.username}}:{{inputs.postgresql.password}}@{{inputs.postgresql.hostname}}:{{inputs.postgresql.port}}/{{inputs.postgresql.database}}')
  table_name = '{{inputs.s3.filename}}'.rsplit('/', 1)[1].rsplit('.', 1)[0]

  # Create postgresql table dynamically using input schema, forcing column names to lowercase to avoid mismatches
  df.columns = df.columns.str.lower()
  df.to_sql(table_name, engine, index=False, if_exists='replace', schema='{{inputs.postgresql.schema_name}}')

except Exception as e:
  print(f"Erreur lors de l'ouverture du fichier : {e}")