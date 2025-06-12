import io
import os
import boto3
import configparser

BUCKET_NAME = "aws-glue-assets-385122037390-us-west-1"
KEY_PATH = "config/config.ini"
def read_config(section, bucket=BUCKET_NAME, key=KEY_PATH):
    s3 = boto3.client('s3')
    obj = s3.get_object(Bucket=bucket, Key=key)
    config_data = obj['Body'].read().decode('utf-8')
    config = configparser.ConfigParser()
    config.read_file(io.StringIO(config_data))
    return dict(config[section])

# def read_config(section: str):
#     config = configparser.ConfigParser()
#     base_dir = os.path.join( os.path.dirname( __file__ ), '..' )
#     config_path  = os.path.abspath(os.path.join(base_dir, "config.ini"))
#     config.read(config_path)
#     if section not in config.sections():
#         raise KeyError(f"Could not find section {section} in config.ini")
    
#     return config[section]
