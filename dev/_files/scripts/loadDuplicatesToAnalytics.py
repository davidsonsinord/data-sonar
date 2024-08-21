import sys
import pandas as pd
import re
from sqlalchemy import create_engine, text

_uri = sys.argv[1]
_username = sys.argv[2]
_password = sys.argv[3]
_hostname = sys.argv[4]
_port = sys.argv[5]
_database = sys.argv[6]
_ingestion_schema = sys.argv[7]
_analytics_schema = sys.argv[8]
_filename = sys.argv[9]

INPUT_SCHEMA = _ingestion_schema
OUTPUT_SCHEMA = _analytics_schema
INPUT_TABLENAME = _filename.rsplit('/', 1)[1].rsplit('.', 1)[0]
TABLE_SUFFIX = '_distinct_duplicates_details'
OUTPUT_TABLENAME = INPUT_TABLENAME + TABLE_SUFFIX

engine = create_engine('postgresql+psycopg2://'+_username+':'+_password+'@'+_hostname+':'+_port+'/'+_database)


df = pd.read_csv(_uri)
with engine.connect() as connection:
  with connection.begin():
    drop_output_table_query = text(f'DROP TABLE IF EXISTS "{OUTPUT_SCHEMA}"."{OUTPUT_TABLENAME}"')
    connection.execute(drop_output_table_query)
    create_output_table_query = text(f'CREATE TABLE IF NOT EXISTS "{OUTPUT_SCHEMA}"."{OUTPUT_TABLENAME}" AS SELECT * FROM "{INPUT_SCHEMA}"."{INPUT_TABLENAME}" LIMIT 0;')
    connection.execute(create_output_table_query)
    for index, value in df['duplicate_row'].items():
      insert_query = text(f'INSERT INTO "{OUTPUT_SCHEMA}"."{OUTPUT_TABLENAME}" VALUES {value};')
      connection.execute(insert_query)