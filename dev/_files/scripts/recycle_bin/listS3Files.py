from kestra import Kestra
import boto3
from botocore.client import Config
import json

# Init S3 client using boto3
s3 = boto3.client('s3',
                  endpoint_url='{{inputs.s3.endpoint_url}}',
                  aws_access_key_id='{{inputs.s3.access_key_id}}',
                  aws_secret_access_key='{{inputs.s3.secret_access_key}}',
                  config=Config(signature_version='s3v4'),
                  verify=False)

try:
  response = s3.list_objects_v2(Bucket='{{inputs.s3.bucket_name}}', Prefix='{{inputs.s3.prefix}}')
  listed_files = []
  for obj in response.get('Contents', []):
    if obj['Key'] != '{{inputs.s3.prefix}}' and obj['Key'].rsplit('.', 1)[1] == '{{inputs.s3.file_extension}}':
      listed_files.append(obj['Key'])
  json_listed_files = json.dumps(listed_files, default = str)
  with open('listed-files.json', "w") as f:
    f.write(json_listed_files)
  Kestra.outputs({'filenames': json_listed_files})
except Exception as e:
    print(f"Erreur lors de l'ouverture du fichier : {e}")