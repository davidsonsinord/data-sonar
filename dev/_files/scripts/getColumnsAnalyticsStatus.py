import sys
import pandas as pd
import re
from sqlalchemy import create_engine, text
import numpy as np

_username = sys.argv[1]
_password = sys.argv[2]
_hostname = sys.argv[3]
_port = sys.argv[4]
_database = sys.argv[5]
_ingestion_schema = sys.argv[6]
_analytics_schema = sys.argv[7]
_filename = sys.argv[8]

engine = create_engine('postgresql+psycopg2://'+_username+':'+_password+'@'+_hostname+':'+_port+'/'+_database)
INPUT_SCHEMA = _ingestion_schema
OUTPUT_SCHEMA = _analytics_schema
INPUT_TABLENAME = _filename.rsplit('/', 1)[1].rsplit('.', 1)[0]
TABLE_SUFFIX = '_columns_analytics_status'
OUTPUT_TABLENAME = INPUT_TABLENAME + TABLE_SUFFIX

data = pd.read_sql_table(INPUT_TABLENAME, engine, schema=_ingestion_schema)

# Fonction qui détermine : le nom des colonnes, le type des colonnes, anomalies sur les colonnes, exemples de valeurs
def columns_analytics_status(df):
    results = []

    for column in df.columns:
        column_type = df[column].dtype
        has_missing_values = df[column].isnull().any()


        # Détection des outliers
        if np.issubdtype(column_type, np.number):
            z_scores = np.abs((df[column] - df[column].mean()) / df[column].std())
            threshold = 3  # Le seuil de z-score au-delà duquel un point est considéré comme un outlier
            has_outliers = (z_scores > threshold).any()
        else:
            has_outliers = False
        
        # Définir le type d'anomalie
        if has_missing_values and has_outliers:
            anomaly_type = 'missing_values, outliers'
        elif has_missing_values:
            anomaly_type = 'missing values'
        elif has_outliers:
            anomaly_type = 'outliers'
        else:
            anomaly_type = ' '
        
        anomaly = 1 if anomaly_type != ' ' else 0
        
        ## Verification du type des colonnes
        column_type_str = 'integer' if np.issubdtype(column_type, np.integer) else \
                          'float' if np.issubdtype(column_type, np.floating) else \
                          'date' if np.issubdtype(column_type, np.datetime64) else \
                          'string' if column_type == object else str(column_type)
        
        # Sélection de 5 valeurs non nulles distinctes aléatoires
        non_null_values = df[column].dropna().unique()
        distinct_values = pd.Series(non_null_values).sample(min(5, len(non_null_values)), random_state=1).astype(str)
        example_values = ', '.join(distinct_values)
        
        results.append([column, column_type_str, anomaly, anomaly_type, example_values])

    # Création du DataFrame des résultats
    df = pd.DataFrame(results, columns=['Name', 'Type of Data', 'Anomaly','Type of Anomaly','Example of values'])

    return df

## on recupere le resultat de la fonction dans le dataframe 
df = pd.DataFrame(data)
res = columns_analytics_status(df)

# Insertion du DataFrame dans la table PostgreSQL
with engine.connect() as connection:
    res.to_sql(OUTPUT_TABLENAME, connection, schema=OUTPUT_SCHEMA, if_exists='replace', index=False)

