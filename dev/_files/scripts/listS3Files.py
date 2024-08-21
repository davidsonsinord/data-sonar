from kestra import Kestra
import boto3
from botocore.client import Config
import json
import sys

_endpoint_url = sys.argv[1]
_access_key_id = sys.argv[2]
_secret_access_key = sys.argv[3]
_bucket_name = sys.argv[4]
_prefix = sys.argv[5]
_file_extension = sys.argv[6]

# Init S3 client using boto3
s3 = boto3.client('s3',
                  endpoint_url=_endpoint_url,
                  aws_access_key_id=_access_key_id,
                  aws_secret_access_key=_secret_access_key,
                  config=Config(signature_version='s3v4'),
                  verify=False)

try:
  response = s3.list_objects_v2(Bucket=_bucket_name, Prefix=_prefix)
  listed_files = []
  for obj in response.get('Contents', []):
    if obj['Key'] != _prefix and obj['Key'].rsplit('.', 1)[1] == _file_extension:
      listed_files.append(obj['Key'])
  json_listed_files = json.dumps(listed_files, default = str)
  with open('listed-files.json', "w") as f:
    f.write(json_listed_files)
  # Kestra.outputs({'filenames': json_listed_files})
except Exception as e:
    print(f"Erreur lors de l'ouverture du fichier : {e}")