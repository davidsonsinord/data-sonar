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
INPUT_TABLENAME = _filename.rsplit('/', 1)[1].rsplit('.', 1)[0]

data = pd.read_sql_table(INPUT_TABLENAME, engine, schema=_ingestion_schema)

def get_numeric_columns(df):
    # Sélectionner les colonnes de type integer et float
    numeric_columns = df.select_dtypes(include=['int64', 'float64']).columns.tolist()
    return numeric_columns

def detect_outliers(df, columns, threshold=3):
    for column in columns:
        # Calculer les scores Z pour la colonne
        z_scores = np.abs((df[column] - df[column].mean()) / df[column].std())
        
        # Créer une nouvelle colonne pour marquer les outliers
        outlier_column = f'{column}_outlier'
        df[outlier_column] = (z_scores > threshold).astype(int)
    return df.filter(regex='outlier')

## recuperation des types de colonnes
numeric_columns = get_numeric_columns(data)

##################  recuperation des outliers
outliers = detect_outliers(data, numeric_columns)
print(outliers)
# Identifier les colonnes contenant 'outlier'
outlier_columns = [col for col in data.columns if 'outlier' in col]
data['potential_outlier'] = (data[outlier_columns] == 1).any(axis=1).astype(int)
